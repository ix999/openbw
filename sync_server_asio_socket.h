#ifndef BWGAME_SYNC_SERVER_ASIO_SOCKET_H
#define BWGAME_SYNC_SERVER_ASIO_SOCKET_H

#include "util.h"

#define ASIO_STANDALONE
#include "deps/asio/asio.hpp"

#include <chrono>
#include <functional>
#include <memory>

namespace {
static inline void silence_asio_warnings() {
	(void)asio::error::system_category;
	(void)asio::error::netdb_category;
	(void)asio::error::addrinfo_category;
	(void)asio::error::misc_category;
}
}

namespace bwgame {

template<typename socket_T>
struct sync_server_asio_socket {

	// NOTE: the asio members are declared AFTER send_buffers and clients (below) so they are
	// destroyed FIRST. ~io_service's shutdown destroys every still-pending async operation,
	// and each destroyed operation's bound handler releases against those containers
	// (async_handle_t decrements client_t::async_count and may erase from `clients`;
	// message_buffer_handle splices `send_buffers`). With the old order the containers were
	// already freed and every game segfaulted post-END in async_release — the teardown crash
	// that also ate unflushed stdout (the 2026-08-08 buffer-masking incident).

	template<typename T, typename release_F>
	struct async_handle_t {
		T* obj;
		release_F release_f;
		async_handle_t(T* obj, release_F&& release_f) : obj(obj), release_f(release_f) {
			++obj->async_count;
		}
		~async_handle_t() {
			if (!--obj->async_count) release_f(obj);
		}
		async_handle_t(const async_handle_t& n) : obj(n.obj), release_f(n.release_f) {
			++obj->async_count;
		}
		operator T*() const {
			return obj;
		}
		T* get() const {
			return obj;
		}
	};
	template<typename T, typename release_F>
	async_handle_t<T, release_F> async_handle(T* obj, release_F&&f) {
		return async_handle_t<T, release_F>(obj, std::forward<release_F>(f));
	}
	
	const size_t recv_size = 0x1000;
	
	struct send_buffer_t {
		std::array<uint8_t, 0x2000> buffer;
		int refcount = 0;
		size_t pos = 0;
	};
	
	using send_buffers_t = a_list<send_buffer_t>;
	send_buffers_t send_buffers;
	
	struct message_buffer_handle {
		sync_server_asio_socket* server = nullptr;
		typename send_buffers_t::iterator buffer;
		size_t offset = 0;
		size_t size = 0;
		message_buffer_handle() = default;
		message_buffer_handle(sync_server_asio_socket& server, typename send_buffers_t::iterator buffer) : server(&server), buffer(buffer) {
			++buffer->refcount;
		}
		message_buffer_handle(const message_buffer_handle& n) {
			server = n.server;
			buffer = n.buffer;
			offset = n.offset;
			size = n.size;
			if (server) ++buffer->refcount;
		}
		message_buffer_handle& operator=(const message_buffer_handle& n) {
			server = n.server;
			buffer = n.buffer;
			offset = n.offset;
			size = n.size;
			if (server) ++buffer->refcount;
			return *this;
		}
		~message_buffer_handle() {
			if (server && --buffer->refcount == 0) {
				server->send_buffers.splice(server->send_buffers.begin(), server->send_buffers, buffer);
			}
		}
	};
	
	struct client_t {
		client_t(socket_T socket) : socket(std::move(socket)) {}
		typename a_list<client_t>::iterator my_it;
		socket_T socket;
		int async_count = 0;
		a_vector<uint8_t> recv_buffer;
		size_t recv_message_size = 0;
		a_deque<message_buffer_handle> send_queue;
		bool is_dead = false;
		std::function<void()> on_kill;
		std::function<void(const void*, size_t)> on_message;
		bool allow_send = false;
		bool write_in_flight = false;
		std::vector<asio::const_buffer> gather_bufs;
	};
	
	a_list<client_t> clients;

	// Teardown, part two. Declaration order alone CANNOT make this safe, because the two
	// constraints are mutually exclusive:
	//   * pending async handlers reference `clients` and `send_buffers`, so ~io_service (which
	//     destroys those handlers) must run FIRST — that is the note above, and the fix for the
	//     2026-08-08 async_release crash;
	//   * each client's socket must deregister its descriptor from the reactor OWNED by that
	//     same io_service, so the reactor must outlive the sockets — the exact opposite.
	// With the current order every process died the other way instead: ~client_t -> socket
	// destructor -> kqueue_reactor::deregister_descriptor -> pthread_mutex_lock on a freed
	// reactor mutex (EXC_BAD_ACCESS at 0x90). Both two-process games crashed at exit, all day,
	// unseen because the message goes to the harness's stderr rather than the game log.
	//
	// This destructor breaks the deadlock by removing the second constraint before either
	// member is touched: a destructor body runs BEFORE member destruction, and a socket that is
	// already closed has no descriptor left to deregister. Teardown-only — it runs after the
	// final frame, so it cannot affect game state or determinism.
	~sync_server_asio_socket() {
		// Same idiom as kill_client below: this wrapper's close() takes no error_code.
		for (auto& c : clients)
			if (c.socket.is_open()) c.socket.close();
	}

	// Destroyed before send_buffers/clients — see the note at the top of the struct.
	asio::io_service io_service;
	asio::io_service::work work{io_service};
	asio::steady_timer timer{io_service};

	typename send_buffers_t::iterator get_send_buffer_with_space(size_t n) {
		for (auto i = send_buffers.begin(); i != send_buffers.end(); ++i) {
			if (i->buffer.size() - i->pos >= n) return i;
		}
		send_buffers.emplace_back();
		return std::prev(send_buffers.end());
	}
	
	struct message_t {
		sync_server_asio_socket& server;
		static_vector<message_buffer_handle, 2> buffers;
		size_t total_size = 0;
		template<typename T>
		void put(T v) {
			std::array<uint8_t, sizeof(T)> buf;
			data_loading::set_value_at<true>(buf.data(), v);
			put(buf.data(), buf.size());
		}
		void put(const void* data, size_t size) {
			auto* buf = &*buffers.back().buffer;
			size_t left = buf->buffer.size() - buf->pos;
			if (left >= size) {
				memcpy(buf->buffer.data() + buf->pos, data, size);
				buf->pos += size;
				buffers.back().size += size;
				total_size += size;
			} else {
				memcpy(buf->buffer.data() + buf->pos, data, left);
				buf->pos += left;
				buffers.back().size += left;
				total_size += left;
				if (buffers.size() == buffers.max_size()) error("message_t: too much data :(");
				buffers.emplace_back(server, server.get_send_buffer_with_space(size - left));
				buffers.back().offset = buffers.back().buffer->pos;
				put((const void*)((const char*)data + left), size - left);
			}
		}
	};
	
	message_t new_message() {
		message_t r{*this};
		auto buffer = get_send_buffer_with_space(0x10);
		r.buffers.emplace_back(*this, buffer);
		r.buffers.back().offset = buffer->pos;
		r.template put<uint16_t>(0);
		return r;
	}
	
	void write_handler(client_t* c, const asio::error_code& ec, size_t bytes_transferred) {
		if (ec) {
			// SB_KILL_LOG companion (task #35): name the socket error that triggers a kill.
			static const bool sb_sock_log = [] {
				const char* v = std::getenv("SB_KILL_LOG");
				return v && *v && *v != '0';
			}();
			if (sb_sock_log)
				std::printf("SBSOCK write-error '%s'\n", ec.message().c_str());
			if (c->on_kill) c->on_kill();
		} else {
			size_t n = bytes_transferred;
			while (n) {
				if (c->send_queue.empty()) error("write_handler: bytes_transferred > queued bytes");
				auto& v = c->send_queue.front();
				size_t take = v.size < n ? v.size : n;
				v.offset += take;
				v.size -= take;
				n -= take;
				if (v.size == 0) c->send_queue.pop_front();
			}
			c->write_in_flight = false;
			if (!c->send_queue.empty()) send_send_queue(c);
		}
	}

	void send_send_queue(client_t* client) {
		// Gather every queued message range into one scatter write: the payloads are tiny
		// (frame syncs ~4 bytes) so the per-write syscall dominates. The byte order on the
		// wire is exactly the queue order — the peer's parser sees an identical stream.
		auto& bufs = client->gather_bufs;
		bufs.clear();
		for (auto& v : client->send_queue) {
			if (bufs.size() == 64) break;
			bufs.emplace_back(v.buffer->buffer.data() + v.offset, v.size);
		}
		client->write_in_flight = true;
		client->socket.async_write_some(bufs, std::bind(&sync_server_asio_socket::write_handler, this, async_handle(client, std::bind(&sync_server_asio_socket::async_release, this, std::placeholders::_1)), std::placeholders::_1, std::placeholders::_2));
	}

	void send_to(const message_t& d, client_t* client) {
		if (!client->allow_send) return;
		for (auto& v : d.buffers) {
			client->send_queue.push_back(v);
		}
	}

	// Deferred-send flush: sends queue in send_to and go to the wire here, so a frame's worth
	// of messages becomes one write. Called on entry to poll()/run_one() — before any wait can
	// begin — and again after their new-client callbacks, so nothing queued can outlive the
	// event-loop boundary that follows it.
	void flush_sends() {
		for (auto& c : clients) {
			if (c.is_dead) continue;
			if (!c.write_in_flight && !c.send_queue.empty()) send_send_queue(&c);
		}
	}
	
	void allow_send(const void* h, bool allow) {
		((client_t*)h)->allow_send = allow;
	}
	
	void send_message(const message_t& d, const void* h) {
		data_loading::set_value_at<true>(d.buffers[0].buffer->buffer.data() + d.buffers[0].offset, (uint16_t)(d.total_size - 2));
		if (h) {
			send_to(d, (client_t*)h);
		} else {
			for (auto& v : clients) {
				send_to(d, &v);
			}
		}
	}
	
	a_vector<client_t*> new_clients;
	
	void new_connection_handler(socket_T socket) {
		clients.emplace_back(std::move(socket));
		client_t* c = &clients.back();
		c->my_it = std::prev(clients.end());
            ++c->async_count;
		new_clients.push_back(c);
	}
	
	void kill_client(const void* h) {
		client_t* c = (client_t*)h;
		c->is_dead = true;
		c->on_kill = {};
		c->on_message = {};
		if (c->socket.is_open()) c->socket.close();
		if (--c->async_count == 0) async_release(c);
	}
	
	template<typename duration_T, typename callback_F>
	void set_timeout(duration_T&& duration, callback_F&& callback) {
		timer.expires_from_now(duration);
		timer.async_wait([callback = std::forward<callback_F>(callback)](const asio::error_code& ec) {
			if (!ec) callback();
		});
	}
	
	void async_release(client_t* c) {
		clients.erase(c->my_it);
	}
	
	void read_handler(client_t* c, const asio::error_code& ec, size_t bytes_transferred) {
		if (ec) {
			static const bool sb_sock_log = [] {
				const char* v = std::getenv("SB_KILL_LOG");
				return v && *v && *v != '0';
			}();
			if (sb_sock_log)
				std::printf("SBSOCK read-error '%s'\n", ec.message().c_str());
			if (c->on_kill) c->on_kill();
		} else {
			c->recv_buffer.resize(c->recv_buffer.size() - recv_size + bytes_transferred);

			while (true) {
				if (c->recv_message_size == 0) {
					if (c->recv_buffer.size() >= 2) {
						data_loading::data_reader_le r((uint8_t*)c->recv_buffer.data(), (uint8_t*)c->recv_buffer.data() + c->recv_buffer.size());
						c->recv_message_size = r.get<uint16_t>();
						c->recv_buffer.erase(c->recv_buffer.begin(), c->recv_buffer.begin() + 2);
					} else break;
				} else if (c->recv_buffer.size() >= c->recv_message_size) {
					if (c->on_message) c->on_message(c->recv_buffer.data(), c->recv_message_size);
					c->recv_buffer.erase(c->recv_buffer.begin(), c->recv_buffer.begin() + c->recv_message_size);
					c->recv_message_size = 0;
				} else break;
			}
			
			size_t new_size = c->recv_buffer.size() + recv_size;
			c->recv_buffer.resize(new_size);
			c->socket.async_read_some(asio::buffer(c->recv_buffer.data() + c->recv_buffer.size() - recv_size, recv_size), std::bind(&sync_server_asio_socket::read_handler, this, async_handle(c, std::bind(&sync_server_asio_socket::async_release, this, std::placeholders::_1)), std::placeholders::_1, std::placeholders::_2));
		}
	}
	
	template<typename F>
	void set_on_kill(const void* h, F&& f) {
		client_t* c = (client_t*)h;
		c->on_kill = std::forward<F>(f);
	}
	
	template<typename F>
	void set_on_message(const void* h, F&& f) {
		client_t* c = (client_t*)h;
		c->on_message = std::forward<F>(f);
		c->recv_buffer.resize(recv_size);
		c->socket.async_read_some(asio::buffer(c->recv_buffer), std::bind(&sync_server_asio_socket::read_handler, this, async_handle(c, std::bind(&sync_server_asio_socket::async_release, this, std::placeholders::_1)), std::placeholders::_1, std::placeholders::_2));
	}
	
	template<typename on_new_client_F>
	void poll(on_new_client_F&& on_new_client) {
		flush_sends();
		io_service.poll();
		for (auto* c : new_clients) {
			c->allow_send = true;
			on_new_client(c);
		}
		new_clients.clear();
		flush_sends();
	}

	template<typename on_new_client_F>
	void run_one(on_new_client_F&& on_new_client) {
		flush_sends();
		if (!io_service.run_one()) error("asio io_service has no work");
		for (auto* c : new_clients) {
			c->allow_send = true;
			on_new_client(c);
		}
		new_clients.clear();
		flush_sends();
	}
	
	template<typename on_new_client_F, typename pred_F>
	void run_until(on_new_client_F&& on_new_client, pred_F&& pred) {
		while (!pred()) {
			run_one(on_new_client);
		}
	}
};

}

#endif
