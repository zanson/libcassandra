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
 * @param[in] t1 Moment as timeval
 * @param[in] t2 Reference moment as timevale
 * @return Delta time in seconds, will be negative if t2 > t1
 */
float
timeval_seconds_delta(const struct timeval & t1, const struct timeval & t2);


/**
 * Converts timeval struct to human readable format
 * @param[in] tv Value to be converted
 * @return Text human readable representation of tv
 */
std::string
human_readable_timeval( const struct timeval & tv, const char * format = "%Y-%m-%d %H:%M:%S");


inline
std::ostream &  operator<<  (std::ostream & os, const struct timeval & tv) 

{
	os << human_readable_timeval(tv);
	return os;
}


/**
 * Calculates delta to curent time 
 * If return value is positive means tv is in future
 * @param[in] tv Moment as timeval
 * @return Delta time in seconds, if moment is in past will be negative, if in future positive
 */
float
timeval_now_seconds_delta( const struct timeval & tv);

#endif // LIBCASSANDRA_UTIL_TIMETOOL_H