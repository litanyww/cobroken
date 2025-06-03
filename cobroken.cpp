
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
#include <variant>

#define DBGOUT(...)                                                        \
    do                                                                     \
    {                                                                      \
        std::ostringstream ost;                                            \
        ost << std::this_thread::get_id() << " - " << __VA_ARGS__ << "\n"; \
        std::cerr << ost.str();                                            \
    } while (0)

constexpr bool withFix = true;

std::stop_source stop{};

class HistoryAdd
{
    void *from{};
    void *to{};
    std::thread::id t{std::this_thread::get_id()};

public:
    HistoryAdd(std::coroutine_handle<> f, std::coroutine_handle<> t) : from{f.address()}, to{t.address()} {}
    void Show(std::ostream &ost) const
    {
        ost << t << " transfer from " << from << " to " << to;
    }
};

class HistoryTChain
{
    void *co{};
    void *dest{};
    std::thread::id t{std::this_thread::get_id()};

public:
    HistoryTChain(std::coroutine_handle<> c, std::coroutine_handle<> d) : co{c.address()}, dest{d.address()} {}
    void Show(std::ostream &ost) const
    {
        ost << t << " thread local chain from " << co << " to " << dest;
    }
};
class HistoryTWoke
{
    void *co{};
    std::thread::id t{std::this_thread::get_id()};

public:
    HistoryTWoke(std::coroutine_handle<> c) : co{c.address()} {}
    void Show(std::ostream &ost) const
    {
        ost << t << " thread local woke " << co;
    }
};

class HistoryResume
{
    void *co{};
    std::thread::id t{std::this_thread::get_id()};

public:
    HistoryResume(std::coroutine_handle<> c) : co{c.address()} {}
    void Show(std::ostream &ost) const
    {
        ost << t << " await_resume " << co;
    }
};

class HistorySuspend
{
    void *co{};
    bool how{};
    std::thread::id t{std::this_thread::get_id()};

public:
    HistorySuspend(std::coroutine_handle<> c, bool h) : co{c.address()}, how{h} {}
    void Show(std::ostream &ost) const
    {
        ost << t << (how ? " suspending " : " resuming ") << co;
    }
};

class HistoryBusted
{
    void *co{};
    std::thread::id t{std::this_thread::get_id()};

public:
    HistoryBusted(std::coroutine_handle<> c) : co{c.address()} {}
    void Show(std::ostream &ost) const
    {
        ost << t << " busted " << co;
    }
};

class History
{
    using Type = std::variant<HistoryAdd, HistoryResume, HistorySuspend, HistoryBusted, HistoryTChain, HistoryTWoke>;
    inline static constexpr size_t maximum = 2000UL;
    std::vector<Type> vec_{};
    unsigned int offset_{};
    mutable std::mutex mutex_{};

public:
    History() { vec_.reserve(maximum); }
    template <typename Entry>
    void Record(Entry &&entry)
    {
        std::scoped_lock guard{mutex_};
        if (vec_.size() < maximum)
        {
            vec_.push_back(std::forward<Entry>(entry));
            ++offset_;
            return;
        }
        offset_ = (offset_ % maximum);
        Type &item = vec_[offset_];
        ++offset_;
        item = std::forward<Entry>(entry);
    }
    void Show()
    {
        std::scoped_lock guard{mutex_};
        auto vec = std::move(vec_);
        unsigned o = (offset_ + (maximum * 2) - vec.size());
        offset_ = 0U;

        DBGOUT(" --- history " << vec.size() << " ---");

        std::ostringstream ost;
        for (size_t i = 0U; i < vec.size(); ++i)
        {
            std::visit([&ost](auto &&arg) mutable
                       { arg.Show(ost); }, vec[--o % maximum]);
            ost << "\n";
        }
        std::cerr << ost.str();
    }
};

History history{};

namespace
{
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
        bool watch_{};
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

class Handles
{
    mutable std::mutex mutex_{};
    std::deque<CoroutineHandle> handles_{};

public:
    void Add(CoroutineHandle h)
    {
        std::scoped_lock guard{mutex_};
        handles_.push_back(h);
    }

    size_t size() const
    {
        std::scoped_lock guard{mutex_};
        return handles_.size();
    }

    CoroutineHandle Get()
    {
        std::scoped_lock guard{mutex_};
        if (handles_.empty())
        {
            return {};
        }
        auto h = handles_.back();
        handles_.pop_back();
        return h;
    }
};

class Fix
{
    class Co
    {
    public:
        struct promise_type
        {
            Co get_return_object() { return Co{std::coroutine_handle<promise_type>::from_promise(*this)}; }
            std::suspend_always initial_suspend() noexcept { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void unhandled_exception() {}
            void return_void() {}
        };

        Co(std::coroutine_handle<promise_type> handle) : handle_{handle} {}
        ~Co() { handle_.destroy(); }
        std::coroutine_handle<> GetHandle() { return handle_; }

    private:
        std::coroutine_handle<promise_type> handle_{};
    };

    class Awaiter
    {
        std::coroutine_handle<> m_destination{};
        std::coroutine_handle<> m_me{};
        std::thread::id m_thread{std::this_thread::get_id()};

    public:
        Awaiter(std::coroutine_handle<> destination) : m_destination{destination} {}
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> me) const noexcept
        {
            history.Record(HistoryTChain(me, m_destination));
            return m_destination;
        }
        void await_resume() const noexcept
        {
            HistoryTWoke(Fix::Instance().m_destination);
        }
    };

private:
    Fix() = default;

public:
    Co Run()
    {
        std::thread::id correctThread{std::this_thread::get_id()};
        for (;;)
        {
            co_await Awaiter{m_destination};
            if (std::this_thread::get_id() != correctThread)
            {
                DBGOUT(" XXX wrong thread, dropping " << m_destination.address());
                CoroutineHandle h{CoroutineHandle::from_address(m_destination.address())};
                h.promise().watch_ = true;
                m_lost.Add(h);
                co_await std::suspend_always();
                DBGOUT(" XXX back after suspend_always");
            }
        }
    }

    static Fix &Instance()
    {
        static thread_local Fix fix{};
        return fix;
    }

    std::coroutine_handle<> Chain(std::coroutine_handle<> next)
    {
        if constexpr (withFix)
        {
            m_destination = next;
            return m_task.GetHandle();
        }
        else
        {
            return next;
        }
    }

    CoroutineHandle GetLost()
    {
        return m_lost.Get();
    }

private:
    inline static thread_local std::coroutine_handle<> m_destination{};
    Co m_task{Run()};
    Handles m_lost{};
};

bool success{true};

class Transfer
{
    Handles &handles_;
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
        me_ = me;
        if (me != mine_)
        {
            DBGOUT(" XXX suspend is suspending the wrong coroutine " << me.address() << " when we thought we were in " << mine_.address());
        }
        me.promise().suspendedBy_ = std::this_thread::get_id();
        me.promise().suspended_.store(true);
        history.Record(HistorySuspend(me, true));
        std::coroutine_handle<> destination = destination_;
        handles_.Add(me);
        ++transferCount;
        return Fix::Instance().Chain(destination);
    }
    bool await_resume()
    {
        if (!me_)
        {
            DBGOUT("awaiter did not suspend!");
        }
        history.Record(HistoryResume{me_});
        if (!me_.promise().suspended_.exchange(false))
        {
            DBGOUT("waking " << me_.address() << " when it was not suspended");
            return false;
        }
        history.Record(HistorySuspend(me_, false));
        return true;
    }
};

class GetMine
{
    CoroutineHandle me_{};

public:
    bool await_ready() { return false; }
    bool await_suspend(CoroutineHandle me)
    {
        me_ = me;
        return false;
    }
    CoroutineHandle await_resume() { return me_; }
};

thread_local CoroutineHandle expected{};

Task Op(std::stop_token token, Handles &handles)
{
    CoroutineHandle mine = co_await GetMine{};
    handles.Add(mine);
    // DBGOUT("starting coroutine " << mine.address());
    co_await std::suspend_always();
    DBGOUT("first " << mine.address());
    while (!token.stop_requested())
    {
        if (CoroutineHandle next = [&]
            { if (CoroutineHandle h = Fix::Instance().GetLost()) { return h; } return handles.Get(); }())
        {
            if (next.promise().watch_)
            {
                DBGOUT("Executing watched coroutine " << next.address());
                next.promise().watch_ = false;
            }
            expected = next;
            history.Record(HistoryAdd{mine, next});
            // DBGOUT(" > " << mine.address() << " transferring to " << next.address());
            auto transferFrom = std::this_thread::get_id();
            next.promise().wakeOn_ = std::this_thread::get_id();
            if (!co_await Transfer{handles, next, mine})
            {
                DBGOUT("unsuspended coroutine started, me=" << mine.address() << ", didn't start " << expected.address());
                success = false;
            }
            if (mine.promise().wakeOn_ != std::this_thread::get_id())
            {
                DBGOUT("unexpectedly did not wake " << mine.address() << " on the expected thread " << mine.promise().wakeOn_);
                success = false;
            }
            if (expected != mine)
            {
                DBGOUT("oops, coroutine " << mine.address() << " woken instead of " << expected.address() << " after " << transferCount.load() << " transfers, transfer from " << transferFrom);
                history.Record(HistoryBusted{mine});
                success = false;
                history.Show();
                stop.request_stop();
            }
        }
        else
        {
            DBGOUT("no handles");
            history.Show();
            co_await std::suspend_always();
            DBGOUT("unexpectedly re-awoken");
        }
    }
}

void Run(std::stop_token token, Handles *handles)
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

bool test()
{
    Handles handles{};
    std::vector<std::jthread> threads_{};
    std::vector<Task> tasks_{};

    for (unsigned int i = 0; i < 7; ++i)
    {
        Task op = Op(stop.get_token(), handles);
        tasks_.push_back(std::move(op));
    }

    for (unsigned int i = 0; i < 3; ++i)
    {
        threads_.push_back(std::jthread{&Run, &handles});
    }
    std::this_thread::sleep_for(std::chrono::seconds{1U});
    std::cerr << "stopping" << std::endl;
    ;
    stop.request_stop();
    std::cout << "success=" << std::boolalpha << success << ", " << transferCount.load() << " transfers" << std::endl;
    return success;
}

#ifndef OMIT_MAIN
int main()
{
    test();
    return 0;
}
#endif