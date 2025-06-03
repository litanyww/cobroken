

#include <atomic>
#include <coroutine>
#include <deque>
#include <mutex>
#include <iostream>
#include <thread>
#include <stop_token>
#include <sstream>
#include <vector>

#define DBGOUT(...) do { std::ostringstream ost; ost << std::this_thread::get_id() << " - " << __VA_ARGS__ << "\n"; std::cerr << ost.str(); } while (0)

std::stop_source stop{};

namespace {
    std::atomic<unsigned long> transferCount{};
}

class Task
{
public:
    struct promise_type
    {
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
public:
    Transfer(Handles &handles, CoroutineHandle destination) : handles_{handles}, destination_{destination}
    {
    }
    bool await_ready() { return false; }
    std::coroutine_handle<> await_suspend(CoroutineHandle me)
    {
        std::coroutine_handle<> destination = destination_;
        handles_.Add(me);
        ++transferCount;
        return destination;
    }
    void await_resume()
    {
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
    co_await std::suspend_always();
    DBGOUT("Starting coroutine " << mine.address());
    while (!token.stop_requested())
    {
        if (CoroutineHandle next = handles.Get())
        {
            expected = next;
            co_await Transfer{handles, next};
            if (expected != mine)
            {
                DBGOUT("oops, coroutine " << mine.address() << " woken instead of " << expected.address() << " after " << transferCount.load() << " transfers");
                success = false;
                stop.request_stop();
            }
        }
        else {
            DBGOUT("no handles");
            co_await std::suspend_always();
        }
    }
}

void Run(std::stop_token token, Handles* handles)
{
    static_cast<void>(token);
    if (CoroutineHandle h = handles->Get())
    {
        expected = h;
        h.resume();
    }
}

bool test()
{
    Handles handles{};
    std::vector<std::jthread> threads_{};
    std::vector<Task> tasks_{};

    for (unsigned int i = 0 ; i < 7 ; ++i)
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
    return success;
}

#ifndef OMIT_MAIN
int main()
{
    test();
    return 0;
}
#endif