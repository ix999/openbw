#ifndef BWGAME_CONTAINERS_H
#define BWGAME_CONTAINERS_H

#include <array>
#include <cstdlib>
#include <cstddef>
#include <memory>
#include <vector>
#include <deque>
#include <list>
#include <set>
#include <map>
#include <unordered_set>
#include <unordered_map>
#include <string>

#include "static_vector.h"
#include "intrusive_list.h"
#include "circular_vector.h"

namespace bwgame {

// Size-class pool behind the engine's allocator alias (ENGINE_REVIEW_2026-08-12 §3.3 / item 4).
// The ladder-ISA profile put the allocator family at 10.1% of process self-instructions; every
// engine container routes through `alloc`, so one pool here reaches all of them. Identity: an
// allocator changes only where storage lives — a_vector/a_list/a_deque iterate in index/insertion
// order regardless, and the digest ladder gates the whole change (the cross-stack unification of
// openbw aaddb02 also proves empirically that no trajectory-relevant pointer-order iteration
// exists). Design: per-thread freelists over power-of-two classes 16..4096 B, refilled from 256 KB
// slabs; larger requests fall through to operator new. The per-thread pool is a LEAKY heap
// singleton — engine teardown may deallocate after thread-local destructors have run, so the pool
// must outlive them; slabs are process-lifetime by design (one game per process).
// SB_ENGINE_POOL_ALLOC=0 is the kill-switch (read once, before any pooled allocation).
class pool_alloc_arena {
	static constexpr size_t kMinShift = 4, kMaxShift = 12;   // 16 B .. 4096 B classes
	static constexpr size_t kClasses = kMaxShift - kMinShift + 1;
	static constexpr size_t kSlabSize = 256 * 1024;
	struct free_node { free_node* next; };
	std::array<free_node*, kClasses> free_ {};
	char* slab_pos_ = nullptr;
	char* slab_end_ = nullptr;
	static size_t class_of(size_t bytes) {
		size_t c = 0;
		while ((size_t(1) << (c + kMinShift)) < bytes) ++c;
		return c;
	}
public:
	static bool enabled() {
		static const bool on = [] {
			const char* v = std::getenv("SB_ENGINE_POOL_ALLOC");
			return !v || v[0] != '0';
		}();
		return on;
	}
	static pool_alloc_arena& instance() {
		static thread_local pool_alloc_arena* a = new pool_alloc_arena();   // leaky by design
		return *a;
	}
	void* allocate(size_t bytes) {
		if (bytes > (size_t(1) << kMaxShift)) return ::operator new(bytes);
		const size_t c = class_of(bytes);
		if (free_[c]) {
			free_node* n = free_[c];
			free_[c] = n->next;
			return n;
		}
		const size_t sz = size_t(1) << (c + kMinShift);
		if (slab_end_ - slab_pos_ < (ptrdiff_t)sz) {
			slab_pos_ = (char*)::operator new(kSlabSize);
			slab_end_ = slab_pos_ + kSlabSize;
		}
		void* r = slab_pos_;
		slab_pos_ += sz;
		return r;
	}
	void deallocate(void* p, size_t bytes) {
		if (bytes > (size_t(1) << kMaxShift)) { ::operator delete(p); return; }
		const size_t c = class_of(bytes);
		free_node* n = static_cast<free_node*>(p);
		n->next = free_[c];
		free_[c] = n;
	}
};

template<typename T>
struct pool_alloc {
	using value_type = T;
	pool_alloc() = default;
	template<typename U> pool_alloc(const pool_alloc<U>&) {}
	T* allocate(size_t n) {
		const size_t bytes = n * sizeof(T);
		if (!pool_alloc_arena::enabled()) return static_cast<T*>(::operator new(bytes));
		return static_cast<T*>(pool_alloc_arena::instance().allocate(bytes));
	}
	void deallocate(T* p, size_t n) {
		const size_t bytes = n * sizeof(T);
		if (!pool_alloc_arena::enabled()) { ::operator delete(p); return; }
		pool_alloc_arena::instance().deallocate(p, bytes);
	}
	template<typename U> bool operator==(const pool_alloc<U>&) const { return true; }
	template<typename U> bool operator!=(const pool_alloc<U>&) const { return false; }
};

template<typename T>
using alloc = std::allocator<T>;

// Node allocator: the pool serves ONLY node-based containers (list/deque blocks, tree and hash
// nodes) — the allocator profile's actual churn — while a_vector/a_string keep std::allocator so
// no interface-crossing type changes (assigning an a_vector to a std::vector, comparing a_string
// to std::string, etc. all stay valid). This keeps the seam one line per container alias below.
template<typename T>
using nalloc = pool_alloc<T>;

template<typename T>
using a_vector = std::vector<T, alloc<T>>;

template<typename T>
using a_deque = std::deque<T, nalloc<T>>;
template<typename T>
using a_list = std::list<T, nalloc<T>>;

template<typename T, typename pred_T = std::less<T>>
using a_set = std::set<T, pred_T, nalloc<T>>;
template<typename T, typename pred_T = std::less<T>>
using a_multiset = std::multiset<T, pred_T, nalloc<T>>;

template<typename key_T, typename value_T, typename pred_T = std::less<key_T>>
using a_map = std::map<key_T, value_T, pred_T, nalloc<std::pair<const key_T, value_T>>>;
template<typename key_T, typename value_T, typename pred_T = std::less<key_T>>
using a_multimap = std::multimap<key_T, value_T, pred_T, nalloc<std::pair<const key_T, value_T>>>;

template<typename T, typename hash_t = std::hash<T>, typename equal_to_t = std::equal_to<T>>
using a_unordered_set = std::unordered_set<T, hash_t, equal_to_t, nalloc<T>>;
template<typename T, typename hash_t = std::hash<T>, typename equal_to_t = std::equal_to<T>>
using a_unordered_multiset = std::unordered_multiset<T, hash_t, equal_to_t, nalloc<T>>;

template<typename key_T, typename value_T, typename hash_t = std::hash<key_T>, typename equal_to_t = std::equal_to<key_T>>
using a_unordered_map = std::unordered_map<key_T, value_T, hash_t, equal_to_t, nalloc<std::pair<const key_T, value_T>>>;
template<typename key_T, typename value_T, typename hash_t = std::hash<key_T>, typename equal_to_t = std::equal_to<key_T>>
using a_unordered_multimap = std::unordered_multimap<key_T, value_T, hash_t, equal_to_t, nalloc<std::pair<const key_T, value_T>>>;

using a_string = std::basic_string<char, std::char_traits<char>, alloc<char>>;

template<typename T>
using a_circular_vector = circular_vector<T, alloc<T>>;

}

#endif
