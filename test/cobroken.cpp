
#include <atomic>
#include <coroutine>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <iostream>
#include <thread>
#include <stop_token>
#include <sstream>
#include <vector>
#include <tuple>
#include <set>

#define DBGOUT(...) do { std::ostringstream ost; ost << std::this_thread::get_id() << " - " << __VA_ARGS__ << "\n"; std::cerr << ost.str(); } while (0)

std::stop_source stop{};

class History
{
    using Type = std::tuple<std::thread::id, void*, void*>;
    inline static constexpr size_t maximum = 200UL;
    std::vector<Type> vec_{};
    unsigned int offset_{};
    mutable std::mutex mutex_{};
public:
    History() { vec_.reserve(maximum); }
    void Record(std::thread::id t, void* a, void* b)
    {
        std::scoped_lock guard{mutex_};
        if (vec_.size() < maximum)
        {
            vec_.emplace_back(t, a, b);
            ++offset_;
            return;
        }
        offset_ = (offset_ % maximum);
        Type& entry = vec_[offset_];
        ++offset_;
        entry = Type{t, a, b};
    }
    void Show() const
    {
        std::scoped_lock guard{mutex_};
        DBGOUT(" --- history " << vec_.size() << " ---");
        unsigned o = (offset_ + (maximum * 2) - vec_.size());
        std::ostringstream ost;
        for (size_t i = 0U ; i < vec_.size() ; ++i)
        {
            auto [t, p1, p2] = vec_[--o % maximum];
            ost << " - " << t << ":  " << p1 << " starts " << p2 << "\n";
        }
        std::cerr << ost.str();
    }
};

History history{};

namespace {
    std::atomic<unsigned long> transferCount{};
}

class Task
{
public:
    struct promise_type
    {
        std::atomic_bool suspended_{};
        std::thread::id suspendedBy_{};
        std::thread::id wakeOn_{};
        Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void unhandled_exception() {}
        void return_void() {}
    };

    Task(std::coroutine_handle<promise_type> handle) : handle_{handle} {}
    std::coroutine_handle<> GetHandle() { return handle_; }

    std::coroutine_handle<promise_type> handle_{};
};
using CoroutineHandle = std::coroutine_handle<Task::promise_type>;

class Watch
{
    mutable std::mutex mutex_{};
    std::set<std::coroutine_handle<>> handles_{};
public:
    void Add(std::coroutine_handle<> h)
    {
        std::scoped_lock guard{mutex_};
        handles_.insert(h);
    }

    bool IsWatched(std::coroutine_handle<> h) const
    {
        std::scoped_lock guard{mutex_};
        if (auto it = handles_.find(h); it != handles_.end()) {
            return true;
        }
        return false;
    }
};

Watch watchlist{};


class Handles
{
    std::mutex mutex_{};
    std::deque<CoroutineHandle> handles_{};
public:
    void Add(CoroutineHandle h)
    {
        std::scoped_lock guard{mutex_};
        handles_.push_back(h);
    }

    CoroutineHandle Get()
    {
        std::scoped_lock guard{mutex_};
        if (handles_.empty()) {
            return {};
        }
        auto h = handles_.back();
        handles_.pop_back();
        return h;
    }
};

bool success{true};

class Transfer
{
    Handles& handles_;
    CoroutineHandle destination_{};
    CoroutineHandle me_{};
    CoroutineHandle mine_{};
public:
    Transfer(Handles &handles, CoroutineHandle destination, CoroutineHandle mine) : handles_{handles}, destination_{destination}, mine_{mine}
    {
    }
    bool await_ready() { return false; }
    std::coroutine_handle<> await_suspend(CoroutineHandle me)
    {
        if (watchlist.IsWatched(me))
        {
            DBGOUT("Adding back a watched handle: " << me.address());
        }
        me_ = me;
        if (me != mine_) {
            DBGOUT(" XXX suspend is suspending the wrong coroutine " << me.address() << " when we thought we were in " << mine_.address());
        }
        me.promise().suspendedBy_ = std::this_thread::get_id();
        me.promise().suspended_.store(true);
        std::coroutine_handle<> destination = destination_;
        handles_.Add(me);
        ++transferCount;
        return destination;
    }
    bool await_resume() {
        if (!me_) {
            DBGOUT("awaiter did not suspend!");
        }
        if (!me_.promise().suspended_.exchange(false))
        {
            DBGOUT("waking " << me_.address() << " when it was not suspended");
            return false;
        }
        return true;

    }
};


class GetMine
{
    CoroutineHandle me_{};
public:
    bool await_ready() { return false; }
    bool await_suspend(CoroutineHandle me) { me_ = me; return false; }
    CoroutineHandle await_resume() { return me_; }
};

thread_local CoroutineHandle expected{};

Task Op(std::stop_token token, Handles& handles)
{
    CoroutineHandle mine = co_await GetMine{};
    handles.Add(mine);
    // DBGOUT("starting coroutine " << mine.address());
    co_await std::suspend_always();
    DBGOUT("first " << mine.address());
    while (!token.stop_requested())
    {
        if (CoroutineHandle next = handles.Get())
        {
            expected = next;
            history.Record(std::this_thread::get_id(), mine.address(), next.address());
            // DBGOUT(" > " << mine.address() << " transferring to " << next.address());
            auto transferFrom = std::this_thread::get_id();
            next.promise().wakeOn_ = std::this_thread::get_id();
            if (watchlist.IsWatched(next))
            {
                DBGOUT("about to wake a watched coroutine " << next.address());
            }
            if (!co_await Transfer{handles, next, mine})
            {
                DBGOUT("unsuspended coroutine started, me=" << mine.address() << ", didn't start " << expected.address());
                success = false;
            }
            if (mine.promise().wakeOn_ != std::this_thread::get_id()) {
                DBGOUT("unexpectedly did not wake " << mine.address() << " on the expected thread " << mine.promise().wakeOn_);
                success = false;
            }
            if (watchlist.IsWatched(mine))
            {
                DBGOUT("somehow, we woke a watched coroutine " << mine.address());
            }
            if (expected != mine)
            {
                DBGOUT("oops, coroutine " << mine.address() << " woken instead of " << expected.address() << " after " << transferCount.load() << " transfers, transfer from " << transferFrom);
                watchlist.Add(expected);
                success = false;
                history.Show();
                stop.request_stop();
            }
        }
        else {
            DBGOUT("no handles");
            co_await std::suspend_always();
            DBGOUT("unexpectedly re-awoken");
        }
    }
}

void Run(std::stop_token token, Handles* handles)
{
    DBGOUT("starting thread");
    static_cast<void>(token);
    if (CoroutineHandle h = handles->Get())
    {
        DBGOUT("thread resuming coroutine " << h.address());
        expected = h;
        h.promise().wakeOn_ = std::this_thread::get_id();
        h.resume();
    }
}

void test()
{
    Handles handles{};
    std::vector<std::jthread> threads_{};
    std::vector<Task> tasks_{};

    for (unsigned int i = 0 ; i < 5 ; ++i)
    {
        Task op = Op(stop.get_token(), handles);
        tasks_.push_back(std::move(op));
    }

    for (unsigned int i = 0 ; i < 3 ; ++i)
    {
        threads_.push_back(std::jthread{&Run, &handles});
    }
    std::this_thread::sleep_for(std::chrono::seconds{1U});
    std::cerr << "stopping" << std::endl;;
    stop.request_stop();
    std::cout << "success=" << std::boolalpha << success << ", " << transferCount.load() << " transfers" << std::endl;
}

int main()
{
    test();
    return 0;
}