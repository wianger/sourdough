#ifndef TIMESTAMP_HH
#define TIMESTAMP_HH

#include <cstdint>
#include <ctime>

/* Current time in milliseconds since the start of the program */
uint64_t timestamp_ms();
uint64_t timestamp_ms(const timespec& ts);

#endif /* TIMESTAMP_HH */
