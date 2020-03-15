//lang::CwC
#pragma once

/**
 * This file is meant to serve as an ever growing set of errors, to minimize repeated code.
 **/

#include <stdlib.h>
#include <cstdio>

const char *RED   = "\033[31m";
const char *RESET = "\033[0m";

static void indexOutOfBounds(size_t index, size_t size) {
    printf("%sError (Index out of bound): %zu is greater than or equal to %zu, the size of this list\n%s", RED, index, size, RESET);
    exit(EXIT_FAILURE);
}

void invariantBreak(const char* invariant) {
    printf("%sError (Invariant breaking): This operation would break the invariant: %s\n%s", RED, invariant, RESET);
    exit(EXIT_FAILURE);
}