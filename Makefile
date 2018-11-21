CFLAGS += -Wall -Wextra
CFLAGS += -g

all: petabuf

clean:
	rm -rf petabuf petabuf.dSYM/

.PHONY: all clean
