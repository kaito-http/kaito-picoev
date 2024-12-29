module picoev

import net
import picohttpparser
import time

// maximum number of file descriptors that can be managed
pub const max_fds = 1024

// maximum size of the event queue
pub const max_queue = 4096

// event for incoming data ready to be read on a socket
pub const picoev_read = 1

// event for socket ready for writing
pub const picoev_write = 2

// event indicating a timeout has occurred
pub const picoev_timeout = 4

// flag for adding a file descriptor to the event loop
pub const picoev_add = 0x40000000

// flag for removing a file descriptor from the event loop
pub const picoev_del = 0x20000000

// event read/write
pub const picoev_readwrite = 3

// Connection phase represents the current state of an HTTP connection
enum ConnectionPhase {
	headers // parsing headers
	body    // reading body
	ended   // request complete
	errored // error occurred
}

// ConnectionState tracks the state of an HTTP connection
pub struct ConnectionState {
pub mut:
	phase        ConnectionPhase
	req          &picohttpparser.Request = unsafe { nil }
	res          &picohttpparser.Response = unsafe { nil }
	buffer       []u8
	buffer_len   int
	content_len  u64
	bytes_read   u64
	chunked      bool
}

// Target is a data representation of everything that needs to be associated with a single
// file descriptor (connection)
pub struct Target {
pub mut:
	fd      int // file descriptor
	loop_id int = -1
	events  u32
	cb      fn (int, int, voidptr) = unsafe { nil }
	// used internally by the kqueue implementation
	backend int
	// connection state for streaming
	state   &ConnectionState = unsafe { nil }
}

// Config configures the Picoev instance with server settings and callbacks
pub struct Config {
pub:
	port         int = 8080
	cb           fn (voidptr, picohttpparser.Request, mut picohttpparser.Response)         = unsafe { nil }
	err_cb       fn (voidptr, picohttpparser.Request, mut picohttpparser.Response, IError) = default_error_callback
	raw_cb       fn (mut Picoev, int, int) = unsafe { nil }
	user_data    voidptr                   = unsafe { nil }
	timeout_secs int                       = 8
	max_headers  int                       = 100
	max_read     int                       = 4096
	max_write    int                       = 8192
	family       net.AddrFamily            = .ip6
	host         string
}

// Core structure for managing the event loop and connections.
// Contains event loop, file descriptor table, timeouts, buffers, and configuration.
@[heap]
pub struct Picoev {
	cb             fn (voidptr, picohttpparser.Request, mut picohttpparser.Response)         = unsafe { nil }
	error_callback fn (voidptr, picohttpparser.Request, mut picohttpparser.Response, IError) = default_error_callback
	raw_callback   fn (mut Picoev, int, int) = unsafe { nil }

	timeout_secs int
	max_headers  int = 100
	max_read     int = 4096
	max_write    int = 8192

	err_cb fn (voidptr, picohttpparser.Request, mut picohttpparser.Response, IError) = default_error_callback @[deprecated: 'use `error_callback` instead']
	raw_cb fn (mut Picoev, int, int) = unsafe { nil } @[deprecated: 'use `raw_callback` instead']
mut:
	loop             &LoopType = unsafe { nil }
	file_descriptors [max_fds]&Target
	timeouts         map[int]i64
	num_loops        int

	buf &u8 = unsafe { nil }
	idx [1024]int
	out &u8 = unsafe { nil }

	date string
pub:
	user_data voidptr = unsafe { nil }
}

// init fills the `file_descriptors` array
pub fn (mut pv Picoev) init() {
	// assert max_fds > 0
	pv.num_loops = 0
	for i in 0 .. max_fds {
		pv.file_descriptors[i] = &Target{}
	}
}

// add a file descriptor to the event loop
@[direct_array_access]
pub fn (mut pv Picoev) add(fd int, events int, timeout int, callback voidptr) int {
	if pv == unsafe { nil } || fd < 0 || fd >= max_fds {
		return -1 // Invalid arguments
	}
	mut target := pv.file_descriptors[fd]
	target.fd = fd
	target.cb = callback
	target.loop_id = pv.loop.id
	target.events = 0
	if pv.update_events(fd, events | picoev_add) != 0 {
		if pv.delete(fd) != 0 {
			elog('Error during del')
		}
		return -1
	}
	pv.set_timeout(fd, timeout)
	return 0
}

// del remove a file descriptor from the event loop
@[deprecated: 'use delete() instead']
@[direct_array_access]
pub fn (mut pv Picoev) del(fd int) int {
	return pv.delete(fd)
}

// remove a file descriptor from the event loop
@[direct_array_access]
pub fn (mut pv Picoev) delete(fd int) int {
	if fd < 0 || fd >= max_fds {
		return -1 // Invalid fd
	}
	mut target := pv.file_descriptors[fd]
	trace_fd('remove ${fd}')
	if pv.update_events(fd, picoev_del) != 0 {
		elog('Error during update_events. event: `picoev.picoev_del`')
		return -1
	}
	pv.set_timeout(fd, 0)
	target.loop_id = -1
	target.fd = 0
	target.cb = unsafe { nil } // Clear callback to prevent accidental invocations
	return 0
}

fn (mut pv Picoev) loop_once(max_wait_in_sec int) int {
	pv.loop.now = get_time()
	if pv.poll_once(max_wait_in_sec) != 0 {
		elog('Error during poll_once')
		return -1
	}
	if max_wait_in_sec == 0 {
		// If no waiting, skip timeout handling for potential performance optimization
		return 0
	}
	// Update loop start time again if waiting occurred
	pv.loop.now = get_time()
	pv.handle_timeout()
	return 0
}

// set_timeout sets the timeout in seconds for a file descriptor. If a timeout occurs
// the file descriptors target callback is called with a timeout event
@[direct_array_access; inline]
fn (mut pv Picoev) set_timeout(fd int, secs int) {
	assert fd < max_fds
	if secs == 0 {
		pv.timeouts.delete(fd)
	} else {
		pv.timeouts[fd] = pv.loop.now + secs
	}
}

// handle_timeout loops over all file descriptors and removes them from the loop
// if they are timed out. Also the file descriptors target callback is called with a
// timeout event
@[direct_array_access; inline]
fn (mut pv Picoev) handle_timeout() {
	mut to_remove := []int{}
	for fd, timeout in pv.timeouts {
		if timeout <= pv.loop.now {
			to_remove << fd
		}
	}
	for fd in to_remove {
		target := pv.file_descriptors[fd]
		assert target.loop_id == pv.loop.id
		pv.timeouts.delete(fd)
		unsafe { target.cb(fd, picoev_timeout, &pv) }
	}
}

// accept_callback accepts a new connection from `listen_fd` and adds it to the event loop
fn accept_callback(listen_fd int, events int, cb_arg voidptr) {
	mut pv := unsafe { &Picoev(cb_arg) }
	accepted_fd := accept(listen_fd)
	if accepted_fd == -1 {
		if fatal_socket_error(accepted_fd) == false {
			return
		}
		elog('Error during accept')
		return
	}
	if accepted_fd >= max_fds {
		// should never happen
		close_socket(accepted_fd)
		return
	}
	trace_fd('accept ${accepted_fd}')
	setup_sock(accepted_fd) or {
		elog('setup_sock failed, fd: ${accepted_fd}, listen_fd: ${listen_fd}, err: ${err.code()}')
		pv.error_callback(pv.user_data, picohttpparser.Request{}, mut &picohttpparser.Response{},
			err)
		close_socket(accepted_fd) // Close fd on failure
		return
	}

	// Initialize the ConnectionState
	mut new_req := &picohttpparser.Request{
		fd: accepted_fd
	}
	mut new_res := &picohttpparser.Response{
		fd: accepted_fd
		buf_start: unsafe { pv.out + accepted_fd * pv.max_write }
		buf: unsafe { pv.out + accepted_fd * pv.max_write }
		date: pv.date.str
	}
	mut cs := &ConnectionState{
		phase: .headers
		req: new_req
		res: new_res
		buffer: []u8{len: pv.max_read}
		buffer_len: 0
	}
	mut target := pv.file_descriptors[accepted_fd]
	target.state = cs

	pv.add(accepted_fd, picoev_read, pv.timeout_secs, raw_callback)
}

// close_conn closes the socket `fd` and removes it from the loop
@[inline]
pub fn (mut pv Picoev) close_conn(fd int) {
	if pv.delete(fd) != 0 {
		elog('Error during del')
	}
	close_socket(fd)
}

// raw_callback handles raw events (read, write, timeout) for a file descriptor
@[direct_array_access]
fn raw_callback(fd int, events int, context voidptr) {
	mut pv := unsafe { &Picoev(context) }
	mut target := pv.file_descriptors[fd]
	eprintln('raw_callback: fd=${fd} events=${events}')
	
	if target == unsafe { nil } {
		eprintln('target is nil, closing connection')
		pv.close_conn(fd)
		return
	}

	if events & picoev_timeout != 0 {
		eprintln('timeout event for fd=${fd}')
		trace_fd('timeout ${fd}')
		if !isnil(pv.raw_callback) {
			pv.raw_callback(mut pv, fd, events)
			return
		}
		if target.state != unsafe { nil } {
			target.state.phase = .errored
		}
		pv.close_conn(fd)
		return
	} else if events & picoev_read != 0 {
		eprintln('read event for fd=${fd}')
		pv.set_timeout(fd, pv.timeout_secs)
		if !isnil(pv.raw_callback) {
			pv.raw_callback(mut pv, fd, events)
			return
		}

		mut cs := target.state
		if cs == unsafe { nil } {
			eprintln('connection state is nil, closing connection')
			pv.close_conn(fd)
			return
		}

		mut had_error := false
		mut would_block := false

		// Keep reading data from the socket in a loop
		for {
			available := cs.buffer.len - cs.buffer_len
			if available <= 0 {
				eprintln('buffer is full')
				// Buffer is full - we need to process what we have first
				break
			}

			r := req_read(fd, &cs.buffer[cs.buffer_len], available, 0)
			eprintln('read ${r} bytes')
			if r == 0 {
				// connection closed by peer
				eprintln('connection closed by peer')
				cs.phase = .ended
				pv.close_conn(fd)
				return
			} else if r == -1 {
				if fatal_socket_error(fd) == false {
					eprintln('would block, breaking read loop')
					// Non-fatal error: EAGAIN or EWOULDBLOCK
					would_block = true
					break
				}
				// fatal error
				eprintln('fatal socket error')
				cs.phase = .errored
				had_error = true
				break
			}
			cs.buffer_len += r

			// Now parse incrementally based on the current phase
			match cs.phase {
				.headers { parse_headers_phase(mut cs, mut pv) }
				.body { parse_body_phase(mut cs, mut pv) }
				else {}
			}

			// If we're done or errored, stop reading
			if cs.phase == .ended || cs.phase == .errored {
				eprintln('request phase ${cs.phase}, breaking read loop')
				break
			}
		}

		// Handle any data we've read before closing on error
		if cs.buffer_len > 0 {
			eprintln('Processing remaining ${cs.buffer_len} bytes')
			match cs.phase {
				.headers { parse_headers_phase(mut cs, mut pv) }
				.body { parse_body_phase(mut cs, mut pv) }
				else {}
			}
		}

		if had_error {
			pv.close_conn(fd)
		} else if !would_block {
			// If we didn't get EAGAIN/EWOULDBLOCK, we're done with this connection
			if cs.phase == .ended {
				pv.close_conn(fd)
			}
		}
	} else if events & picoev_write != 0 {
		eprintln('write event for fd=${fd}')
		pv.set_timeout(fd, pv.timeout_secs)
		if !isnil(pv.raw_callback) {
			pv.raw_callback(mut pv, fd, events)
			return
		}
	}
}

fn default_error_callback(data voidptr, req picohttpparser.Request, mut res picohttpparser.Response, error IError) {
	elog('picoev: ${error}')
	res.status(400)
	res.header('Content-Type', 'application/json')
	res.header('Connection', 'close')
	res.body('{"error":"${error}"}')
}

// new creates a `Picoev` struct and initializes the main loop
pub fn new(config Config) !&Picoev {
	listening_socket_fd := listen(config) or {
		elog('Error during listen: ${err}')
		return err
	}
	mut pv := &Picoev{
		num_loops:      1
		cb:             config.cb
		error_callback: config.err_cb
		raw_callback:   config.raw_cb
		user_data:      config.user_data
		timeout_secs:   config.timeout_secs
		max_headers:    config.max_headers
		max_read:       config.max_read
		max_write:      config.max_write
	}
	if isnil(pv.raw_callback) {
		pv.buf = unsafe { malloc_noscan(max_fds * config.max_read + 1) }
		pv.out = unsafe { malloc_noscan(max_fds * config.max_write + 1) }
	}
	// epoll on linux
	// kqueue on macos and bsd
	// select on windows and others
	$if linux {
		pv.loop = create_epoll_loop(0) or { panic(err) }
	} $else $if freebsd || macos {
		pv.loop = create_kqueue_loop(0) or { panic(err) }
	} $else {
		pv.loop = create_select_loop(0) or { panic(err) }
	}
	if pv.loop == unsafe { nil } {
		elog('Failed to create loop')
		close_socket(listening_socket_fd)
		return unsafe { nil }
	}
	pv.init()
	pv.add(listening_socket_fd, picoev_read, 0, accept_callback)
	return pv
}

// serve starts the event loop for accepting new connections
// See also picoev.new().
pub fn (mut pv Picoev) serve() {
	spawn update_date_string(mut pv)
	for {
		pv.loop_once(1)
	}
}

// update_date updates the date field of the Picoev instance every second for HTTP headers
fn update_date_string(mut pv Picoev) {
	for {
		// get GMT (UTC) time for the HTTP Date header
		gmt := time.utc()
		pv.date = gmt.http_header_string()
		time.sleep(time.second)
	}
}

// parse_headers_phase attempts to parse HTTP headers from the connection buffer
fn parse_headers_phase(mut cs ConnectionState, mut pv Picoev) {
	// Attempt to parse headers from cs.buffer
	s := unsafe { cs.buffer[..cs.buffer_len].bytestr() }
	mut req := cs.req
	parsed := req.parse_request(s) or {
		// parse error
		cs.phase = .errored
		pv.error_callback(pv.user_data, req, mut cs.res, err)
		if cs.res.end() < 0 {
			eprintln('Failed to send error response')
		}
		pv.close_conn(req.fd)
		return
	}
	if parsed > 0 {
		// We have complete headers
		cs.phase = .body

		// Detect content-length or chunked
		if content_length := req.get_header('content-length') {
			cs.content_len = content_length.u64()
		}
		if transfer_encoding := req.get_header('transfer-encoding') {
			cs.chunked = transfer_encoding.to_lower() == 'chunked'
		}

		// Remove parsed portion from the buffer
		if parsed < cs.buffer_len {
			leftover := cs.buffer_len - parsed
			unsafe { C.memmove(&cs.buffer[0], &cs.buffer[parsed], leftover) }
			cs.buffer_len = leftover
		} else {
			cs.buffer_len = 0
		}

		// Check if content-length is zero → no body
		if cs.content_len == 0 && !cs.chunked {
			cs.phase = .ended
			// Call the user callback with the complete request
			pv.cb(pv.user_data, req, mut cs.res)
			if cs.res.end() < 0 {
				eprintln('Failed to send response')
			}
			pv.close_conn(req.fd)
		}
	}
}

// parse_body_phase handles reading the HTTP request body
fn parse_body_phase(mut cs ConnectionState, mut pv Picoev) {
	mut req := cs.req
	if cs.chunked {
		// TODO: Implement chunked transfer encoding
		// For now, treat as error
		cs.phase = .errored
		pv.error_callback(pv.user_data, req, mut cs.res, error('Chunked transfer encoding not implemented'))
		if cs.res.end() < 0 {
			eprintln('Failed to send error response')
		}
		pv.close_conn(req.fd)
		return
	} else {
		// content-length approach
		if cs.content_len > 0 {
			// We have new data in cs.buffer
			needed := cs.content_len - cs.bytes_read
			in_buffer := cs.buffer_len
			to_consume := if u64(in_buffer) > needed { int(needed) } else { in_buffer }

			// Update the request body
			req.body = unsafe { cs.buffer[..to_consume].bytestr() }
			cs.bytes_read += u64(to_consume)

			// shift the buffer
			leftover := cs.buffer_len - to_consume
			if leftover > 0 {
				unsafe { C.memmove(&cs.buffer[0], &cs.buffer[to_consume], leftover) }
			}
			cs.buffer_len = leftover

			if cs.bytes_read == cs.content_len {
				cs.phase = .ended
				// Call the user callback with the complete request
				pv.cb(pv.user_data, req, mut cs.res)
				if cs.res.end() < 0 {
					eprintln('Failed to send response')
				}
				pv.close_conn(req.fd)
			}
		}
	}
}