#pragma once

#include <string>
#include <vector>
#include <utility>
#include <random>

namespace redis {

constexpr int ZSKIPLIST_MAXLEVEL = 32;
constexpr double ZSKIPLIST_P = 0.25;

struct SkipListLevel {
    struct SkipListNode* forward{nullptr};
    unsigned long span{0};
};

struct SkipListNode {
    std::string member;
    double score{0.0};
    SkipListNode* backward{nullptr};
    std::vector<SkipListLevel> level;

    SkipListNode(int level_count, double s, std::string m)
        : member(std::move(m)), score(s), level(level_count) {}
};

class SkipList {
public:
    SkipList();
    ~SkipList();

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    SkipList(SkipList&& other) noexcept;
    SkipList& operator=(SkipList&& other) noexcept;

    SkipListNode* insert(double score, const std::string& member);
    bool remove(double score, const std::string& member);
    bool update_score(double cur_score, const std::string& member, double new_score);

    unsigned long get_rank(double score, const std::string& member) const;
    SkipListNode* get_element_by_rank(unsigned long rank) const;
    std::vector<std::pair<std::string, double>> get_range_by_rank(long start, long stop) const;

    size_t size() const { return length_; }
    bool empty() const { return length_ == 0; }
    void clear();

private:
    int random_level();

    SkipListNode* header_{nullptr};
    SkipListNode* tail_{nullptr};
    size_t length_{0};
    int level_{1};
    std::mt19937 rng_;
};

} // namespace redis
