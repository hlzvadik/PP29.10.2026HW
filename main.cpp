#include <functional>
#include <queue>
#include <deque>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <iostream>
#include <chrono>
#include <atomic>
#include <random>
#include <memory>

class th_pool_t
{
public:
  using f_t = std::function< void() >;
  explicit th_pool_t(unsigned int k, std::size_t batch_size = 4);
  ~th_pool_t();

  void submit(f_t f);
  void stop_now();
  void stop_when_done();
  void pause();
  void resume();

private:
  struct worker_t
  {
    std::deque< f_t > local_;
    mutable std::mutex mx_;
    std::thread th_;
    std::size_t id_{0};
  };

  void execute(worker_t& w);
  bool try_take_batch(worker_t& w);
  bool try_steal(worker_t& w);

  std::deque< f_t > tasks_;
  mutable std::shared_mutex tasks_rw_;
  std::condition_variable_any cd_;
  std::condition_variable_any cv_pause_;

  bool stop_{false};
  bool stop_now_{false};
  bool paused_{false};

  std::size_t batch_size_;
  std::vector< std::unique_ptr< worker_t > > workers_;
};

th_pool_t::th_pool_t(unsigned int k, std::size_t batch_size)
  : batch_size_(batch_size)
{
  workers_.reserve(k);
  for (unsigned int i = 0; i <  k; ++i)
  {
    auto w = std::make_unique< worker_t >();
    w->id_ = i;
    worker_t* raw = w.get();
    w->th_ = std::thread([this, raw]() { execute(*raw); });
    workers_.push_back(std::move(w));
  }
}

th_pool_t::~th_pool_t()
{
  stop_when_done();
}

void th_pool_t::pause()
{
  std::unique_lock lock(tasks_rw_);
  paused_ = true;
}

void th_pool_t::resume()
{
  {
    std::unique_lock lock(tasks_rw_);
    paused_ = false;
  }
  cv_pause_.notify_all();
  cd_.notify_all();
}

void th_pool_t::stop_now()
{
  {
    std::unique_lock lock(tasks_rw_);
    stop_now_ = true;
  }
  cv_pause_.notify_all();
  cd_.notify_all();

  for (auto& w : workers_)
    if (w->th_.joinable()) w->th_.join();
  workers_.clear();
}

void th_pool_t::stop_when_done()
{
  {
    std::unique_lock lock(tasks_rw_);
    stop_ = true;
  }
  cv_pause_.notify_all();
  cd_.notify_all();

  for (auto& w : workers_)
    if (w->th_.joinable()) w->th_.join();
  workers_.clear();
}

void th_pool_t::submit(f_t f)
{
  {
    std::unique_lock lock(tasks_rw_);
    tasks_.push_back(std::move(f));
    if (paused_) return;
  }
  cd_.notify_one();
}

bool th_pool_t::try_take_batch(worker_t& w)
{
  {
    std::shared_lock sl(tasks_rw_);
    if (tasks_.empty() || stop_now_) return false;
  }

  std::unique_lock ul(tasks_rw_);
  if (tasks_.empty() || stop_now_) return false;

  std::size_t n = std::min(batch_size_, tasks_.size());
  {
    std::lock_guard lg(w.mx_);
    for (std::size_t i = 0; i <  n; ++i)
    {
      w.local_.push_back(std::move(tasks_.front()));
      tasks_.pop_front();
    }
  }
  return true;
}

bool th_pool_t::try_steal(worker_t& w)
{
  for (auto& victim_ptr : workers_)
  {
    worker_t& victim = *victim_ptr;
    if (&victim == &w) continue;

    std::unique_lock vl(victim.mx_, std::try_to_lock);
    if (!vl.owns_lock()) continue;
    if (victim.local_.size() <  2) continue;

    std::size_t steal_n = victim.local_.size() / 2;

    std::unique_lock lg(w.mx_, std::try_to_lock);
    if (!lg.owns_lock()) continue;

    for (std::size_t i = 0; i <  steal_n; ++i)
    {
      w.local_.push_back(std::move(victim.local_.front()));
      victim.local_.pop_front();
    }
    return true;
  }
  return false;
}

void th_pool_t::execute(worker_t& w)
{
  while (true)
  {
    {
      std::lock_guard lg(w.mx_);
      if (!w.local_.empty())
      {
        auto task = std::move(w.local_.back());
        w.local_.pop_back();
        try { task(); } catch (...) {}
        continue;
      }
    }

    if (try_take_batch(w)) continue;
    if (try_steal(w)) continue;

    std::unique_lock< std::shared_mutex > lck(tasks_rw_);

    if (stop_now_) break;
    if (stop_ && tasks_.empty())
    {
      bool all_empty = true;
      for (auto& wp : workers_)
      {
        std::lock_guard lg(wp->mx_);
        if (!wp->local_.empty()) { all_empty = false; break; }
      }
      if (all_empty) break;
    }

    cd_.wait(lck, [&]() {
      if (stop_now_ || stop_) return true;
      if (paused_) return false;
      return !tasks_.empty();
    });

    if (paused_ && !stop_now_ && !stop_)
    {
      cv_pause_.wait(lck, [&]() {
        return !paused_ || stop_now_ || stop_;
      });
    }
  }
}

int main()
{
  std::atomic< int > done{0};
  std::atomic< int > total{0};

  auto make_task = [&](int ms)
  {
    ++total;
    return [&, ms]()
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(ms));
      ++done;
    };
  };

  constexpr int N = 200;

  {
    th_pool_t pool(4, 4);

    std::mt19937 rng(42);
    std::uniform_int_distribution< int > dist(1, 3);

    auto t0 = std::chrono::steady_clock::now();

    for (int i = 0; i <  N; ++i)
    {
      int kind = dist(rng);
      int ms = (kind == 1) ? 1 : (kind == 2) ? 10 : 50;
      pool.submit(make_task(ms));
    }
    pool.submit(make_task(300));

    pool.stop_when_done();

    auto t1 = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast< std::chrono::milliseconds >(t1 - t0).count();
    std::cout << "done=" << done.load() << "/" << total.load()  << "  elapsed=" << ms << " ms\n";
  }

  std::cout << "final done=" << done.load() << "\n";
  return 0;
}
