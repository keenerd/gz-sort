/*
gz-sort.c is licensed GPLv3
copyright Kyle Keen, 2016

perform a merge sort over a multi-gigabyte gz compressed file

compile: gcc -Wall -Os -o gz-sort gz-sort.c -lz
*/

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <malloc.h>
#include <pthread.h>
#include <zlib.h>

#define CHUNK 16384
#define LINE_START 1024
#define GZ_BUFFER 65536
#define PRESORT_WINDOW 1000000
#define NWAY_WINDOW 1000
#define MAX_THREADS 64

typedef struct
// maintains all state related to the GZ process
{
    char* path;
    gzFile f;
    int read_len;
    char buffer[CHUNK + 1];
    int buf_i;
    char* line;  // dynamically expanded/reused
    char* str;   // points to line or buffer
    int line_len;
    int line_i;
    int64_t subset_counter;
    int64_t line_counter;
    int64_t nway_lines;
    int64_t nway_skips;
} gzBucket;

typedef struct
// holds misc state and settings
{
    char* label;
    int64_t total_lines;
    int64_t presort_bytes;
    int64_t* line_log;  // "pointers" to where each segment starts
    int64_t log_len;
    int pass_through;
    int unique;
    int nway;
} miscBucket;

typedef struct
// thread state
{
    pthread_t sort_thread;
    gzBucket* g;
    char* label;
    char* source_path;
    char* in_path;
    char* out_path;
    int thread_index;
    miscBucket misc;
} threadBucket;

void show_help(void)
{
    fprintf(stdout,
        "perform a merge sort over a multi-GB gz compressed file\n\n"
        "use: gz-sort [-u] [-S n] [-P n] source.gz dest.gz\n\n"
        "options:\n"
        "   -h: help\n"
        "   -u: unique\n"
        "   -S n: size of presort, supports k/M/G suffix\n"
        "         a traditional in-memory sort (default n=1M)\n"
        "   -P n: use multiple threads (experimental, default disabled)\n"
        "   -T: pass through (debugging/benchmarks)\n\n"
        "estimating run time, crudely:\n"
        "    time gzip -dc data.gz | gzip > /dev/null\n"
        "    unthreaded: seconds * entropy * (log2(uncompressed_size/S)+2)\n"
        "    (where 'entropy' is a fudge-factor between 1.5 for an \n"
        "    already sorted file and 3 for a shuffled file)\n"
        "    S and P are the corresponding settings\n"
        "    multithreaded: maybe unthreaded/sqrt(P) ?\n\n"
        "estimated disk use:\n"
        "    2x source.gz\n"
        "\n");
    exit(0);
}

int init_gz(gzBucket* g, char* path, char* mode)
{
    g->line_len = LINE_START;
    g->line_i = 0;
    g->buf_i = 0;
    g->line = NULL;
    if (!(g->f = gzopen(path, mode)))
    {
        fprintf(stderr, "ERROR: %s not a .gz file\n", path) ;  
        return 1 ;  
    } 
    gzbuffer(g->f, GZ_BUFFER);
    g->line = malloc(g->line_len + 1);
    g->subset_counter = 0;
    g->line_counter = 0;

    // seed the read
    if (mode[0] == 'r')
        {g->read_len = gzread(g->f, &g->buffer, CHUNK);}
    return 0;
}

int close_gz(gzBucket* g)
{
    gzclose(g->f);
    free(g->line);
    return 0;
}

int init_all(gzBucket* in1, gzBucket* in2, gzBucket* out, char* path1, char* path2)
{
    if (init_gz(in1, path1, "rb"))
        {return 1;}
    if (init_gz(in2, path1, "rb"))
        {return 1;}
    if (init_gz(out, path2, "wb"))
        {return 1;}
    return 0;
}

int append_line_gz(gzBucket* g, char* str, int length)
// this handles growth
{
    // str does not fit, line must grow
    while (g->line_i + length >= g->line_len)
    {
        g->line_len *= 2;
        g->line = realloc(g->line, g->line_len+1);
    }
    // now str can fit in line
    memcpy(g->line+g->line_i, str, length);
    // advance pointers along
    g->line_i += length;
    return 0;
}

char* load_line_gz(gzBucket* g)
// returns NULL if out of lines
{
    char* found = NULL;
    int i;
    while (g->read_len)
    {
        // out of buffer?  load more
        if (g->buf_i >= g->read_len)
        {
            g->read_len = gzread(g->f, &g->buffer, CHUNK);
            g->buf_i = 0;
        }
        // scan ahead for newline
        for (i=g->buf_i; i<CHUNK; i++)
        {
            if (g->buffer[i] == '\n')
            {
                g->buffer[i] = '\0';
                g->line_counter++;
                break;
            }
        }
        if (i == CHUNK)  // did not find newline, append
        {
            append_line_gz(g, g->buffer + g->buf_i, i-g->buf_i);
        }
        else if (g->line_i)  // cached line, append
        {
            // extra +1 because of reasons
            append_line_gz(g, g->buffer + g->buf_i, i+1-g->buf_i);
            found = g->line;
            g->line_i = 0;  // awkward...
        }
        else // re-use buffer
        {
            found = g->buffer + g->buf_i;
        }
        // advance indexes along
        g->buf_i = i + 1;
        if (found)
        {
            g->str = found;
            return found;
        }
    }
    return NULL;
}

char* subset_lines_gz(gzBucket* g)
// returns NULL when subset_counter hits zero
{
    if (g->subset_counter <= 0)
        {return NULL;}
    g->subset_counter--;
    return load_line_gz(g);
}

int skip_lines_gz(gzBucket* g, int skip)
{
    int i;
    for (i=0; i<skip; i++)
        {load_line_gz(g);}
    return 0;
}

char* nway_line_gz(gzBucket* g)
// performs the nway_chop while emulating load_line_gz
// requires that you setup subset_counter & skip beforehand
// returns NULL when out of lines
{
    if (g->subset_counter <= 0)
    {
        skip_lines_gz(g, g->nway_skips);
        g->subset_counter = g->nway_lines;
    }
    return subset_lines_gz(g);
}

int report_time(char* message, int start)
{
    time_t finish = time(NULL);
    int seconds = finish - start;
    float minutes;
    if (seconds <= 1)
        {return 0;}
    if (seconds < 100)
    {
        fprintf(stdout, "%s: %i seconds\n", message, seconds);
        return seconds;
    }
    minutes = (float)seconds / 60;
    fprintf(stdout, "%s: %.2f minutes\n", message, minutes);
    return seconds;
}

int simple_pass(gzBucket* in1, gzBucket* out)
{
    char* str1;
    while (1)
    {
        str1 = load_line_gz(in1);
        if (str1 == NULL)
            {break;}
        gzputs(out->f, str1);
        gzputs(out->f, "\n");
    }
    return 0;
}

int pass_through_pass(char* input_path, char* output_path)
{
    gzBucket in1;
    gzBucket in2;
    gzBucket out;
    time_t start;
    if (init_all(&in1, &in2, &out, input_path, output_path))
        {return 1;}
    start = time(NULL);
    simple_pass(&in1, &out);
    report_time("passthrough", start);
    close_gz(&in1); close_gz(&in2); close_gz(&out);
    return 0;
}

int qsort_compare(const void* a, const void* b)
{
    const char** str1 = (const char **)a;
    const char** str2 = (const char **)b;
    if (*str1 == NULL)
        {return 1;}
    if (*str2 == NULL)
        {return -1;}
    return strcmp(*str1, *str2);
}

int presort_pass(gzBucket* in1, gzBucket* out, miscBucket* misc, char* line_gz(gzBucket*))
// updates line_log with how many lines were processed
{
    char* buffer;  // fixed length
    char** strings;  // grows
    char* str1;
    int eof, eob, str1_len;
    int64_t i, lines, strings_len, buf_i, log_i, strings_i;
    eof = 0;
    in1->line_counter = 0;
    // largest malloc, most likely to OOM
    buffer = malloc(sizeof(char) * (misc->presort_bytes+1));
    if (buffer == NULL)
        {return 1;}
    strings_len = 1024;
    strings = malloc(sizeof(char*) * (strings_len+1));
    log_i = 0;
    lines = 0;
    strings_i = 0;
    buf_i = 0;
    for (i=0; i<misc->log_len+1; i++)
        {misc->line_log[i] = -1;}
    while (!eof)
    {
        eob = 0;
        while (!eob)
        {
            // load a line
            str1 = line_gz(in1);
            if (str1 == NULL)
                {eof = 1; break;}
            // does buffer have space for the string?
            str1_len = strlen(str1);
            if (str1_len+1 >= misc->presort_bytes)
                {fprintf(stderr, "WARNING: buffer too small\n");}
            if (buf_i + str1_len + 1 >= misc->presort_bytes)
            {
                eob = 1;
                break;
            }
            // does strings have space for another pointer?
            if (strings_i+3 >= strings_len)
            {
                strings_len *= 2;
                strings = realloc(strings, sizeof(char*) * (strings_len+1));
            }
            strcpy(buffer+buf_i, str1);
            strings[strings_i] = buffer+buf_i;
            buf_i += str1_len + 1;
            strings_i++;
        }
        // sort and write out
        qsort(strings, strings_i, sizeof(char*), qsort_compare);
        lines = 0;
        for (i=0; i<strings_i; i++)
        {
            if (strings[i] == NULL)
                {continue;}
            gzputs(out->f, strings[i]);
            gzputs(out->f, "\n");
            out->line_counter++;
            strings[i] = NULL;
            lines++;
        }
        // save the line count
        if (log_i+3 >= misc->log_len)
        {
            misc->log_len *= 2;
            misc->line_log = realloc(misc->line_log, sizeof(int64_t) * (misc->log_len+1));
            for (i=log_i; i<misc->log_len+1; i++)
                {misc->line_log[i] = -1;}
        }
        misc->line_log[log_i] = lines;
        log_i++;
        lines = 0;
        // put the loose str1 back in
        strings_i = 0;
        buf_i = 0;
        if (str1 != NULL)
        {
            strcpy(buffer+buf_i, str1);
            strings[strings_i] = buffer+buf_i;
            buf_i += strlen(str1) + 1;
            strings_i++;
        }
    }
    // clean up
    free(buffer);
    free(strings);
    return 0;
}

int nway_chop_and_presort(char* in_path, char* out_path, threadBucket* t, miscBucket* misc)
{
    time_t start;
    char* report;
    gzBucket in1;
    gzBucket out;
    start = time(NULL);
    // set up the gz files
    if (init_gz(&in1, in_path, "rb"))
        {return 1;}
    init_gz(&out, out_path, "wb");
    // set up the offsets
    skip_lines_gz(&in1, NWAY_WINDOW * t->thread_index);
    in1.subset_counter = NWAY_WINDOW;
    in1.nway_lines = NWAY_WINDOW;
    in1.nway_skips = NWAY_WINDOW * (misc->nway-1);
    // do a normal presort
    // except it needs nway_line_gz() instead of load_line_gz()
    in1.line_counter = 0;
    out.line_counter = 0;
    misc->log_len = 1024;
    misc->line_log = malloc(sizeof(int64_t) * (misc->log_len+1));
    if (presort_pass(&in1, &out, misc, &nway_line_gz))
        {return 1;}
    //misc->total_lines = in1.line_counter + NWAY_WINDOW * t->thread_index;
    misc->total_lines = out.line_counter;
    // clean up
    close_gz(&in1); close_gz(&out);
    asprintf(&report, "%s line count: %ld\n%s %s", misc->label, out.line_counter, misc->label, "chop/presort");
    report_time(report, start);
    free(report);
    return 0;
}

int first_pass(char* input_path, char* output_path, miscBucket* misc)
// updates total_lines in misc
{ 
    gzBucket in1;
    gzBucket in2;
    gzBucket out;
    time_t start;
    char* report;
    char* label2 = "";
    if (init_all(&in1, &in2, &out, input_path, output_path))
        {return 1;}
    start = time(NULL);
    in1.line_counter = 0;
    misc->log_len = 1024;
    misc->line_log = malloc(sizeof(int64_t) * (misc->log_len+1));
    if (presort_pass(&in1, &out, misc, &load_line_gz))
        {return 1;}
    label2 = "presort";
    asprintf(&report, "%s line count: %ld\n%s %s", misc->label, in1.line_counter, misc->label, label2);
    misc->total_lines = in1.line_counter;
    report_time(report, start);
    free(report);
    close_gz(&in1); close_gz(&in2); close_gz(&out);
    return 0;
}

int merge_pass(gzBucket* in1, gzBucket* in2, gzBucket* out, miscBucket* misc, int unique)
{
    // now with line_log, it is okay to unique during any pass
    char* str1;
    char* str2;
    char* str3;
    int cmp, log_i;
    int64_t size1, size2;
    log_i = 0;
    size1 = misc->line_log[log_i];
    skip_lines_gz(in2, size1);
    while (1)
    {
        size1 = misc->line_log[log_i];
        size2 = misc->line_log[log_i+1];
        in1->subset_counter = size1;
        in2->subset_counter = size2;
        str1 = subset_lines_gz(in1);
        str2 = subset_lines_gz(in2);
        if (str1==NULL && str2==NULL)
            {break;}
        while (str1!=NULL || str2!=NULL)
        {
            cmp = 0;
            str3 = NULL;
            // normal merge sort, chip away at either
            if (str1!=NULL && str2!=NULL)
            {
                cmp = strcmp(str1, str2);
                if (cmp < 0)
                    {str3 = str1;}
                else
                    {str3 = str2;}
            }
            // one chunk is empty, pass the rest through
            if (str1!=NULL && str2==NULL)
            {
                cmp = -1;
                str3 = str1;
            }
            if (str1==NULL && str2!=NULL)
            {
                cmp = 1;
                str3 = str2;
            }
            if (!unique)
            {
                gzputs(out->f, str3); gzputs(out->f, "\n");
                out->line_counter++;
            }
            else if (strcmp(str3, out->line)!=0)
            {
                gzputs(out->f, str3); gzputs(out->f, "\n");
                out->line_i = 0;
                append_line_gz(out, str3, strlen(str3));
                out->line[out->line_i] = '\0';
                out->line_counter++;
            }
            if (cmp < 0)
                {str1 = subset_lines_gz(in1);}
            else
                {str2 = subset_lines_gz(in2);}
        }
        misc->line_log[log_i]   = -1;
        misc->line_log[log_i+1] = -1;
        misc->line_log[log_i/2] = size1 + size2;
        if (size1 == -1 || size2 == -1)
            {misc->line_log[log_i/2]++;}
        log_i += 2;
        skip_lines_gz(in1, size2);  // old size2
        size1 = misc->line_log[log_i];
        skip_lines_gz(in2, size1);
    }
    return 0;
}

int64_t typical_segment(miscBucket* misc)
// average number of lines to be merged
{
    int64_t i, total, size;
    total = 0;
    size = 0;
    for (i=0; i < misc->log_len; i++)
    {
        if (misc->line_log[i] == -1)
            {break;}
        total += misc->line_log[i];
        size++;
    }
    if (size == 0)
        {return -1;}
    return total / size;
}

int middle_passes(char* input_path, char* output_path, miscBucket* misc)
// updates size in misc
{
    gzBucket in1;
    gzBucket in2;
    gzBucket out;
    int unique = 0;
    int64_t average = 0;
    int64_t line_counter = 0;
    time_t start;
    char* report;
    while (misc->line_log[1] != -1)
    {
        // last pass
        if (misc->line_log[2] == -1)
            {unique = misc->unique;}

        if (init_all(&in1, &in2, &out, input_path, output_path))
            {return 1;}

        start = time(NULL);
        average = typical_segment(misc);
        merge_pass(&in1, &in2, &out, misc, unique);
        asprintf(&report, "%s merge %ld", misc->label, average);
        report_time(report, start);
        free(report);
        line_counter = out.line_counter;
        close_gz(&in1); close_gz(&in2); close_gz(&out);
        rename(output_path, input_path);
    }
    if (misc->unique)
        {fprintf(stdout, "removed %ld non-unique lines\n",
            misc->total_lines - line_counter);}
    return 0;
}

#define heap_parent(x) ((x-1) / 2)
#define heap_child1(x) (x*2 + 1)
#define heap_child2(x) (x*2 + 2)

int heap_add(char* heap[], char* str, int heap_tail)
// manually increment the tail afterwards
{
    int p, c;
    c = heap_tail;
    heap[c] = str;
    while (c > 0)
    {
        p = heap_parent(c);
        if (strcmp(heap[p], str) <= 0)
            {break;}
        // move it up
        heap[c] = heap[p];
        heap[p] = str;
        c = p;
    }
    return 0;
}

int heap_pop(char* heap[], int heap_tail)
// manually decrement the tail afterwards
{
    int p, c, c1, c2;
    char* last = heap[heap_tail - 1];
    p = 0;
    heap[p] = last;
    heap[heap_tail - 1] = NULL;
    if (heap_tail == 1)
        {return 0;}
    while (p < heap_tail)
    {
        c1 = heap_child1(p);
        c2 = heap_child2(p);
        // cleanest hack for avoiding strcmp(NULL)
        if (heap[c1] == NULL)
            {c1 = p;}
        if (heap[c2] == NULL)
            {c2 = p;}
        if (strcmp(heap[c1], heap[c2]) > 0)
            {c = c2;}
        else
            {c = c1;}
        if (strcmp(heap[p], heap[c]) <= 0)
            {break;}
        // move it down
        heap[p] = heap[c];
        heap[c] = last;
        p = c;
    }
    return 0;
}

int nway_merge_pass(threadBucket* nway_table, char* out_path, miscBucket* misc)
// simpler version that merges fully sorted files
{
    char* strs[MAX_THREADS+1];
    gzBucket out;
    time_t start;
    char* report;
    char* str;
    int i, count, heap_tail;
    int64_t total_lines = 0;
    heap_tail = 0;
    count = misc->nway;
    start = time(NULL);
    // set up all the files
    init_gz(&out, out_path, "wb");
    for (i=0; i<count; i++)
    {
        nway_table[i].g = malloc(sizeof(gzBucket));
        init_gz(nway_table[i].g, nway_table[i].out_path, "rb");
    }
    // seed the string array
    for (i=0; i<MAX_THREADS; i++)
        {strs[i] = NULL;}
    for (i=0; i<count; i++)
        {heap_add(strs, load_line_gz(nway_table[i].g), heap_tail++);}
    while (strs[0] != NULL)
    {
        if (!misc->unique)
            {gzputs(out.f, strs[0]); gzputs(out.f, "\n");}
        else if (strcmp(strs[0], out.line)!=0)
        {
            gzputs(out.f, strs[0]); gzputs(out.f, "\n");
            out.line_i = 0;
            append_line_gz(&out, strs[0], strlen(strs[0]));
            out.line[out.line_i] = '\0';
            out.line_counter++;
        }
        // find which it came from and replace it
        // (this is kind of crude, but pointer checks are fast)
        for (i=0; i<count; i++)
        {
            if (strs[0] != nway_table[i].g->str)
                {continue;}
            heap_pop(strs, heap_tail--);
            str = load_line_gz(nway_table[i].g);
            if (str == NULL)
                {break;}
            heap_add(strs, str, heap_tail++);
            break;
        }
    }

    asprintf(&report, "%i-way merge", misc->nway);
    report_time(report, start);
    free(report);
    for (i=0; i<count; i++)
        {total_lines += nway_table[i].misc.total_lines;}
    if (misc->unique)
        {fprintf(stdout, "removed %ld non-unique lines\n",
            total_lines - out.line_counter);}
    // clean up all the files
    close_gz(&out);
    for (i=0; i<count; i++)
    {
        close_gz(nway_table[i].g);
        free(nway_table[i].g);
    }
    return 0;
}

static void* sort_thread_fn(void* arg)
// problem, can't modify misc...
{
    threadBucket* t = arg;
    miscBucket* misc = &(t->misc);
    char* temp_path = t->in_path;
    char* output_path = t->out_path;
    misc->total_lines = 0;
    misc->label = t->label;

    // first pass is a doozy
    if (nway_chop_and_presort(t->source_path, temp_path, t, misc))
        {return NULL;}

    // merge sort everything
    middle_passes(temp_path, output_path, misc);
    rename(temp_path, output_path);

    return NULL;
}

int main(int argc, char **argv)
{
    miscBucket misc;
    threadBucket nway_table[MAX_THREADS];
    char* input_path;
    char* output_path;
    char* temp_path;
    int i, optchar;
    char suffix;
    misc.pass_through = 0;
    misc.unique = 0;
    misc.nway = 0;
    misc.label = "";
    misc.log_len = 0;
    misc.presort_bytes = PRESORT_WINDOW;
    mallopt(M_MMAP_THRESHOLD, 1);

    while ((optchar = getopt(argc, argv, "huTS:P:")) != -1)
    {
        switch (optchar)
        {
            case 'u':
                misc.unique = 1;
                break;
            case 'T':
                misc.pass_through = 1;
                break;
            case 'P':
                misc.nway = atoi(optarg);
                if (misc.nway > MAX_THREADS)
                    {misc.nway = MAX_THREADS;}
                break;
            case 'S':
                misc.presort_bytes = (int64_t)atoi(optarg);
                suffix = optarg[strlen(optarg)-1];
                if (suffix == 'k' || suffix == 'K')
                    {misc.presort_bytes *= 1000;}
                if (suffix == 'M')
                    {misc.presort_bytes *= 1000000;}
                if (suffix == 'G')
                    {misc.presort_bytes *= 1000000000;}
                break;
            case 'h':
                show_help();
            default:
                show_help();
                exit(2);
                break;
        }
    }

    if (argc != optind+2)
        {show_help();}
    if (!misc.presort_bytes)
        {show_help();}
    if (misc.nway)
        {misc.presort_bytes /= misc.nway;}
    input_path = argv[optind];
    output_path = argv[optind+1];

    // fudge factor, suspected PEBCAK
    if (misc.presort_bytes < 1e9)
        {misc.presort_bytes /= 2;}
    else
        {misc.presort_bytes -= 0.5e9;}

    // debug mode
    if (misc.pass_through)
        {return pass_through_pass(input_path, output_path);}

    // simple un-threaded sort
    if (!misc.nway)
    {
        asprintf(&temp_path, "%s.temp", output_path);
        if (first_pass(input_path, output_path, &misc))
            {return 1;}
        rename(output_path, temp_path);

        middle_passes(temp_path, output_path, &misc);
        rename(temp_path, output_path);

        free(temp_path);
        return 0;
    }
    // multi thread sort
    // set up the data for each process
    for (i=0; i < misc.nway; i++)
    {
        nway_table[i].misc.nway = misc.nway;
        nway_table[i].misc.presort_bytes = misc.presort_bytes;
        nway_table[i].thread_index = i;
        nway_table[i].source_path = input_path;
        asprintf(&(nway_table[i].label), "T%i", i+1);
        asprintf(&(nway_table[i].in_path), "%s.T%i.temp", output_path, i+1);
        asprintf(&(nway_table[i].out_path), "%s.T%i.gz",  output_path, i+1);
    }
    // run all the sorts
    for (i=0; i < misc.nway; i++)
    {
        // combined chop, presort and extra middle_pass
        pthread_create(&nway_table[i].sort_thread, NULL, sort_thread_fn, (void *)(&nway_table[i]));
        //sort_thread_fn((void *)(&nway_table[i]));
    }
    // wait for threads, merge everything and clean up
    for (i=0; i < misc.nway; i++)
    {
        if (nway_table[i].sort_thread)
            {pthread_join(nway_table[i].sort_thread, NULL);}
        unlink(nway_table[i].in_path);
    }
    nway_merge_pass(nway_table, output_path, &misc);
    for (i=0; i < misc.nway; i++)
    {
        unlink(nway_table[i].out_path);
    }
    return 0;
}
