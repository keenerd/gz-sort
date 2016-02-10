# gz-sort - sort multi-GB compressed files

# -ggdb -std=c99
CFLAGS := -Os -std=gnu99 -Wall -Werror -pedantic -Wextra -pthread ${CFLAGS}
LDLIBS  = -lz -lpthread

%.a: %.o
	strip -d -X $<
	ar rvs $@ $<

all: gz-sort strip

gz-sort: gz-sort.o

strip: gz-sort
	strip --strip-all $^

clean:
	$(RM) *.o gz-sort
	$(RM) tests/*.gz

test: gz-sort
	$(RM) tests/*.gz
	./tests/_setup.sh
	./tests/_run-all.sh
	$(RM) tests/*.gz

.PHONY: all clean strip test
