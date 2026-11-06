#include <functional>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>

class th_pool_t
{
public:
  using f_t = std::function<void()>;
  explicit th_pool_t(unsigned int k);
  ~th_pool_t();

  void submit(f_t f);
  void stop_now();
  void stop_when_done();
  void pause();
  void resume();

private:
  void execute();

  std::queue<f_t> tasks_;
  std::mutex mx_;
  std::condition_variable cd_;
  std::condition_variable cv_pause_;

  bool stop_{false};
  bool stop_now_{false};
  bool paused_{false};

  std::vector<std::thread> ths_;
};

th_pool_t::th_pool_t(unsigned int k)
{
  for (; k; --k)
    ths_.emplace_back([this]() { execute(); });
}

void th_pool_t::pause()
{
  std::lock_guard lock(mx_);
  paused_ = true;
}

void th_pool_t::resume()
{
  {
    std::lock_guard lock(mx_);
    paused_ = false;
  }
  cv_pause_.notify_all(); 
  cd_.notify_all(); 
}

void th_pool_t::stop_now()
{
  {
    std::lock_guard lock(mx_);
    stop_now_ = true;
  }
  cv_pause_.notify_all();
  cd_.notify_all();

  for (auto&& th : ths_)
  {
    th.join();
  }
  ths_.clear();
}

void th_pool_t::stop_when_done()
{
  {
    std::lock_guard lock(mx_);
    stop_ = true;
  }
  cv_pause_.notify_all();
  cd_.notify_all();

  for (auto&& th : ths_)
  {
    th.join();
  }
  ths_.clear();
}

th_pool_t::~th_pool_t()
{
  stop_when_done();
}

void th_pool_t::submit(f_t f)
{
  {
    std::lock_guard lock(mx_);
    tasks_.push(std::move(f));
    if (paused_)
    {
      return;
    }
  }
  cd_.notify_one();
}

void th_pool_t::execute()
{
  std::unique_lock lck(mx_);

  while (true)
  {
    cd_.wait(lck, [&]() {
      return !tasks_.empty() || stop_ || stop_now_;
    });

    if (stop_now_) break;
    if (stop_ && tasks_.empty()) break;

    if (paused_)
    {
      cv_pause_.wait(lck, [&]() {
        return !paused_ || stop_now_ || stop_; 
      });
      continue; 
    }

    if (!tasks_.empty())
    {
      auto task = std::move(tasks_.front());
      tasks_.pop();

      lck.unlock();
      try
      {
        task();
      }
      catch (...)
      {}
      lck.lock();
    }
  }
}

int main()
{
}
