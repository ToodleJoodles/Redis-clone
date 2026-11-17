#include "skip_list.hpp"

namespace redis {

SkipList::SkipList() : rng_(std::random_device{}()) {
    header_ = new SkipListNode(ZSKIPLIST_MAXLEVEL, 0.0, "");
    for (int i = 0; i < ZSKIPLIST_MAXLEVEL; ++i) {
        header_->level[i].forward = nullptr;
        header_->level[i].span = 0;
    }
    tail_ = nullptr;
    length_ = 0;
    level_ = 1;
}

SkipList::~SkipList() {
    clear();
    delete header_;
    header_ = nullptr;
}

SkipList::SkipList(SkipList&& other) noexcept : rng_(std::random_device{}()) {
    header_ = other.header_;
    tail_ = other.tail_;
    length_ = other.length_;
    level_ = other.level_;

    other.header_ = new SkipListNode(ZSKIPLIST_MAXLEVEL, 0.0, "");
    for (int i = 0; i < ZSKIPLIST_MAXLEVEL; ++i) {
        other.header_->level[i].forward = nullptr;
        other.header_->level[i].span = 0;
    }
    other.tail_ = nullptr;
    other.length_ = 0;
    other.level_ = 1;
}

SkipList& SkipList::operator=(SkipList&& other) noexcept {
    if (this != &other) {
        clear();
        delete header_;

        header_ = other.header_;
        tail_ = other.tail_;
        length_ = other.length_;
        level_ = other.level_;

        other.header_ = new SkipListNode(ZSKIPLIST_MAXLEVEL, 0.0, "");
        for (int i = 0; i < ZSKIPLIST_MAXLEVEL; ++i) {
            other.header_->level[i].forward = nullptr;
            other.header_->level[i].span = 0;
        }
        other.tail_ = nullptr;
        other.length_ = 0;
        other.level_ = 1;
    }
    return *this;
}

void SkipList::clear() {
    if (!header_) return;
    SkipListNode* node = header_->level[0].forward;
    while (node) {
        SkipListNode* next = node->level[0].forward;
        delete node;
        node = next;
    }
    for (int i = 0; i < ZSKIPLIST_MAXLEVEL; ++i) {
        header_->level[i].forward = nullptr;
        header_->level[i].span = 0;
    }
    tail_ = nullptr;
    length_ = 0;
    level_ = 1;
}

int SkipList::random_level() {
    int lvl = 1;
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    while (dist(rng_) < ZSKIPLIST_P && lvl < ZSKIPLIST_MAXLEVEL) {
        lvl++;
    }
    return lvl;
}

SkipListNode* SkipList::insert(double score, const std::string& member) {
    SkipListNode* update[ZSKIPLIST_MAXLEVEL];
    unsigned long rank[ZSKIPLIST_MAXLEVEL];
    SkipListNode* x = header_;

    for (int i = level_ - 1; i >= 0; --i) {
        rank[i] = (i == (level_ - 1)) ? 0 : rank[i + 1];
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score && x->level[i].forward->member < member))) {
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    int lvl = random_level();
    if (lvl > level_) {
        for (int i = level_; i < lvl; ++i) {
            rank[i] = 0;
            update[i] = header_;
            update[i]->level[i].span = length_;
        }
        level_ = lvl;
    }

    x = new SkipListNode(lvl, score, member);
    for (int i = 0; i < lvl; ++i) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    for (int i = lvl; i < level_; ++i) {
        update[i]->level[i].span++;
    }

    x->backward = (update[0] == header_) ? nullptr : update[0];
    if (x->level[0].forward) {
        x->level[0].forward->backward = x;
    } else {
        tail_ = x;
    }
    length_++;
    return x;
}

bool SkipList::remove(double score, const std::string& member) {
    SkipListNode* update[ZSKIPLIST_MAXLEVEL];
    SkipListNode* x = header_;

    for (int i = level_ - 1; i >= 0; --i) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score && x->level[i].forward->member < member))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    x = x->level[0].forward;
    if (x && x->score == score && x->member == member) {
        for (int i = 0; i < level_; ++i) {
            if (update[i]->level[i].forward == x) {
                update[i]->level[i].span += x->level[i].span - 1;
                update[i]->level[i].forward = x->level[i].forward;
            } else {
                update[i]->level[i].span -= 1;
            }
        }
        if (x->level[0].forward) {
            x->level[0].forward->backward = x->backward;
        } else {
            tail_ = x->backward;
        }
        while (level_ > 1 && header_->level[level_ - 1].forward == nullptr) {
            level_--;
        }
        length_--;
        delete x;
        return true;
    }
    return false;
}

bool SkipList::update_score(double cur_score, const std::string& member, double new_score) {
    if (!remove(cur_score, member)) {
        return false;
    }
    insert(new_score, member);
    return true;
}

unsigned long SkipList::get_rank(double score, const std::string& member) const {
    unsigned long rank = 0;
    SkipListNode* x = header_;
    for (int i = level_ - 1; i >= 0; --i) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score && x->level[i].forward->member <= member))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }
        if (x != header_ && x->member == member && x->score == score) {
            return rank;
        }
    }
    return 0;
}

SkipListNode* SkipList::get_element_by_rank(unsigned long rank) const {
    if (rank == 0 || rank > length_) return nullptr;
    unsigned long traversed = 0;
    SkipListNode* x = header_;
    for (int i = level_ - 1; i >= 0; --i) {
        while (x->level[i].forward && (traversed + x->level[i].span <= rank)) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return nullptr;
}

std::vector<std::pair<std::string, double>> SkipList::get_range_by_rank(long start, long stop) const {
    std::vector<std::pair<std::string, double>> result;
    if (length_ == 0) return result;

    if (start < 0) start = static_cast<long>(length_) + start;
    if (stop < 0) stop = static_cast<long>(length_) + stop;

    if (start < 0) start = 0;
    if (start >= static_cast<long>(length_) || start > stop) {
        return result;
    }
    if (stop >= static_cast<long>(length_)) {
        stop = static_cast<long>(length_) - 1;
    }

    unsigned long start_rank = static_cast<unsigned long>(start + 1);
    unsigned long count = static_cast<unsigned long>(stop - start + 1);

    SkipListNode* node = get_element_by_rank(start_rank);
    while (node && count > 0) {
        result.emplace_back(node->member, node->score);
        node = node->level[0].forward;
        count--;
    }
    return result;
}

} // namespace redis
