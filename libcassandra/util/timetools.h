/*
 * LibCassandra
 * Copyright (C) 2011 Mateusz Korniak
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */
#ifndef LIBCASSANDRA_UTIL_TIMETOOL_H
#define LIBCASSANDRA_UTIL_TIMETOOL_H

#include <string>

#include <sys/time.h>


/**
 * Returns delta ( t1 - t2 ) in seconds beetween two timeval values
 * If return value is positive means t1 is later than t2
 */
float
timeval_seconds_delta(const struct timeval & t1, const struct timeval & t2);


/**
 * Returns delta ( t1 - t2 ) in seconds beetween two timeval values
 * If return value is positive means t1 is later than t2
 */

std::string
human_readable_timeval( const struct timeval & tv, const char * format = "%Y-%m-%d %H:%M:%S");

// std::string static
float
human_readable_timeval_now_delta( const struct timeval & tv);

#endif // LIBCASSANDRA_UTIL_TIMETOOL_H