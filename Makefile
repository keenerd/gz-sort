# gz-sort - sort multi-GB compressed files

# -ggdb -std=c99
CFLAGS := -O3 -std=gnu99 -Wall -Werror -pedantic -Wextra -pthread ${CFLAGS}
LDLIBS  = -lz -lpthread
RM     ?= rm -f

.o.a:
	strip -d -X $<
	ar rvs $@ $<

all: strip

strip: gz-sort
	strip --strip-all gz-sort

clean:
	$(RM) *.o gz-sort
	$(RM) tests/*.gz

test: gz-sort
	$(RM) tests/*.gz
	./tests/_setup.sh
	./tests/_run-all.sh
	$(RM) tests/*.gz

.PHONY: all clean strip test
