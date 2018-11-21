/*
 * petabuf.c
 * Copyright (c) 2018, Sijmen J. Mulder <ik@sjmulder.nl>
 *
 * petabuf is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option)
 * any later version.
 *
 * petabuf is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for
 * more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with petabuf. If not, see <https://www.gnu.org/licenses/>.
 */

#if defined(__OpenBSD__) || defined(__APPLE__)
# define USE_SYSCTL
#elif defined(__linux__)
# define USE_SYSINFO
#endif

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <string.h>
#include <assert.h>

#if defined(USE_SYSCTL)
# include <sys/types.h>
# include <sys/sysctl.h>
#elif defined(USE_SYSINFO)
# include <sys/sysinfo.h>
#endif

#include <sys/select.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <err.h>

#define PAGESZ		(1 << 24) /* 16 MiB */
#define TABLESZ		(1 << 26) /* 1 PiB worth of pages */
#define HEADROOM	(PAGESZ * 4)

#define PAGE_MAPPED	0x1
#define PAGE_ONDISK	0x2

struct paddr {
	uint32_t idx;	/* 0..<TABLESZ */
	uint32_t off;	/* 0..<PAGESZ */
};

static void page_pin(uint32_t);
static void page_unpin(uint32_t);
static void page_free(uint32_t);
static char *page_ptr(struct paddr);
static char *page_filepath(uint32_t);

static size_t get_memsize(void);
static void log_counters(void);

static void *headroom;

/* page table */
static char **pages;
static uint8_t *states;

/* counters */
static size_t nmapped;
static size_t nondisk;
static size_t nfree;	/* in memory */

/* cursors */
static struct paddr rpos;
static struct paddr wpos;

int
main(int argc, char **argv)
{
	size_t memsize;
	int flags;
	fd_set read_fds;
	fd_set write_fds;
	size_t ntoread, ntowrite;
	ssize_t nread, nwritten;

	if (getopt(argc, argv, "") != -1)
		return 1;
	if (argc != optind) {
		fputs("usage: ... | petabuf | ...\n", stderr);
		return 1;
	}

	if (!(pages = malloc(sizeof(*pages) * TABLESZ)))
		err(1, "allocating pages");
	if (!(states = malloc(sizeof(*states) * TABLESZ)))
		err(1, "allocating states");
	if (!(headroom = malloc(HEADROOM)))
		err(1, "allocating headroom");

	memsize = get_memsize();
	nfree = memsize / PAGESZ / 2;

	fprintf(stderr, "system reports %zu (%zu GB) of memory, using up to "
	    "half\n", memsize, memsize >> 30);

	if ((flags = fcntl(STDIN_FILENO, F_GETFL)) == -1)
		err(1, "getting flags for stdin");
	if (fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK) == -1)
		err(1, "setting flags for stdin");
	if ((flags = fcntl(STDOUT_FILENO, F_GETFL)) == -1)
		err(1, "getting flags for stdout");
	if (fcntl(STDOUT_FILENO, F_SETFL, flags | O_NONBLOCK) == -1)
		err(1, "setting flags for stdout");

	log_counters();
	page_pin(0);

	ntoread = PAGESZ;
	ntowrite = 0;

	while (ntoread || ntowrite) {
		fprintf(stderr, "rpos=%u+%u, wpos=%u+%u\n",
		    (unsigned)rpos.idx, (unsigned)rpos.off,
		    (unsigned)wpos.idx, (unsigned)wpos.off);

		FD_ZERO(&read_fds);
		FD_ZERO(&write_fds);

		if (ntoread)
			FD_SET(STDIN_FILENO, &read_fds);
		if (ntowrite)
			FD_SET(STDOUT_FILENO, &write_fds);

		if (select(3, &read_fds, &write_fds, 0, NULL) == -1)
			err(1, "select");

		if (FD_ISSET(STDIN_FILENO, &read_fds)) {
			nread = read(STDIN_FILENO, page_ptr(rpos), ntoread);
			if (nread == -1)
				err(1, "read");
			else if (!nread) {
				fprintf(stderr, "end of input\n");
				ntoread = 0;
			} else {
				fprintf(stderr, "read %zd bytes\n", nread);

				if ((rpos.off += (uint32_t)nread) == PAGESZ) {
					if (rpos.idx != wpos.idx)
						page_unpin(rpos.idx);
					if (++rpos.idx >= TABLESZ)
						errx(1, "out of pages");
					page_pin(rpos.idx);
					rpos.off = 0;
				}

				ntoread = PAGESZ - rpos.off;
			}
		}

		if (FD_ISSET(STDOUT_FILENO, &write_fds)) {
			nwritten = write(STDOUT_FILENO, page_ptr(wpos),
			    ntowrite);
			if (nwritten == -1)
				err(1, "write");

			fprintf(stderr, "wrote %zd bytes\n", nwritten);

			if ((wpos.off += (uint32_t)nwritten) == PAGESZ) {
				page_unpin(wpos.idx);
				page_free(wpos.idx);
				page_pin(++wpos.idx);
				wpos.off = 0;
			}
		}

		ntowrite = (wpos.idx == rpos.idx ? rpos.off : PAGESZ)
		    - wpos.off;
	}

	return 0;
}

static void
page_pin(uint32_t idx)
{
	char *path;
	int fd;

	assert(idx < TABLESZ);

	if (states[idx] & PAGE_MAPPED)
		return;

	if (states[idx] & PAGE_ONDISK) {
		/* existing page on disk */

		path = page_filepath(idx);
		if ((fd = open(path, O_RDWR)) == -1)
			err(1, "opening %s", path);

		pages[idx] = mmap(NULL, PAGESZ, PROT_READ | PROT_WRITE,
		    MAP_SHARED, fd, 0);
		if (pages[idx] == MAP_FAILED)
			err(1, "mapping %s", path);
		if (close(fd) == -1)
			err(1, "closing %s", path);

		states[idx] |= PAGE_MAPPED;
		nmapped++;
	} else if (nfree) {
		/* new page in memory */

		pages[idx] = mmap(NULL, PAGESZ, PROT_READ | PROT_WRITE,
		    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
		if (pages[idx] == MAP_FAILED) {
			if (errno != ENOMEM)
				err(1, "allocating page");

			fprintf(stderr, "out of memory, resetting nfree\n");
			nfree = 0;

			if (headroom) {
				fprintf(stderr, "using headroom\n");
				free(headroom);
				headroom = NULL;
			}

			goto ondisk;
		}

		states[idx] |= PAGE_MAPPED;
		nmapped++;
		nfree--;
	} else {
	ondisk:
		/* new page on disk */

		path = page_filepath(idx);
		if ((fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600)) == -1)
			err(1, "creating %s", path);
		if (ftruncate(fd, PAGESZ) == -1)
			err(1, "growing %s", path);

		pages[idx] = mmap(NULL, PAGESZ, PROT_READ | PROT_WRITE,
		    MAP_SHARED, fd, 0);
		if (pages[idx] == MAP_FAILED)
			err(1, "mapping %s", path);
		if (close(fd) == -1)
			err(1, "closing %s", path);

		states[idx] |= PAGE_MAPPED | PAGE_ONDISK;
		nmapped++;
		nondisk++;
	}

	log_counters();
}

static void
page_unpin(uint32_t idx)
{
	assert(idx < TABLESZ);

	if (!(states[idx] & PAGE_MAPPED))
		return;

	if (states[idx] & PAGE_ONDISK) {
		if (munmap(pages[idx], PAGESZ) == -1)
			err(1, "unmapping page");

		states[idx] &= ~PAGE_MAPPED;
		nmapped--;

		log_counters();
	}
}

static void
page_free(uint32_t idx)
{
	char *path;

	assert(idx < TABLESZ);

	if (states[idx] & PAGE_ONDISK) {
		assert(!(states[idx] & PAGE_MAPPED)); /* not pinned */

		path = page_filepath(idx);
		if (unlink(path) == -1)
			err(1, "unlinking %s", path);

		states[idx] &= ~PAGE_ONDISK;
		nondisk--;

		log_counters();
	} else if (states[idx] & PAGE_MAPPED) {
		if (munmap(pages[idx], PAGESZ) == -1)
			err(1, "freeing page");

		states[idx] &= ~PAGE_MAPPED;
		nmapped--;
		nfree++;

		log_counters();
	}
}

static char *
page_ptr(struct paddr addr)
{
	assert(addr.idx < TABLESZ);
	assert(addr.off < PAGESZ);
	assert(states[addr.idx] & PAGE_MAPPED);

	return pages[addr.idx] + addr.off;
}

static char *
page_filepath(uint32_t idx)
{
	static char path[PATH_MAX];
	size_t len;

	assert(idx < TABLESZ);

	len = snprintf(path, sizeof(path), "/tmp/petabuf.%zu", (size_t)idx);
	if (len >= sizeof(path))
		errx(1, "path too long");

	return path;
}

#if defined(USE_SYSCTL)
static size_t
get_memsize(void)
{
	int names[2];
	size_t len;
	uint64_t memsize;

	len = sizeof(memsize);
	names[0] = CTL_HW;
#if defined(HW_USERMEM64)
	names[1] = HW_USERMEM64;
#else
	names[1] = HW_MEMSIZE;
#endif

	if (sysctl(names, 2, &memsize, &len, NULL, 0) == -1)
		err(1, "sysctl");

	return (size_t)memsize;
}
#elif defined(USE_SYSINFO)
static size_t
get_memsize(void)
{
	struct sysinfo info;

	if (sysinfo(&info) == -1)
		err(1, "sysinfo");

	return (size_t)info.totalram;
}
#endif

static void
log_counters(void)
{
	fprintf(stderr, "nmapped=%zu (%zu GB), nondisk=%zu (%zu GB), "
	    "nfree=%zu (%zu GB)\n",
	    nmapped, nmapped >> 6, nondisk, nondisk >> 6, nfree, nfree >> 6);
}
