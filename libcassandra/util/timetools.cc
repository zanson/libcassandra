/*
 * LibCassandra
 * Copyright (C) 2011 Mateusz Korniak
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */
#include <time.h>
#include <stdio.h>

#include <sstream>

#include "timetools.h"

float 
timeval_seconds_delta(const struct timeval & t1, const struct timeval & t2)
{
	struct timeval res;
	//void timersub(struct timeval *a, struct timeval *b,
        //      struct timeval *res);
        timersub(&t1,&t2,&res); // http://linux.die.net/man/3/timersub
	/// NOTE: Seems res.tv_usec is always positive. So negative deltas
	return res.tv_sec + res.tv_usec * 1e-6;
	
}

std::string
human_readable_timeval( const struct timeval & tv, const char * format )
{
	// Taken from 
	// http://stackoverflow.com/questions/2408976/struct-timeval-to-printable-format
	// time_t nowtime;
	
	time_t nowtime = tv.tv_sec;
	struct tm result_tm;
	localtime_r(&nowtime, &result_tm);
	char tmbuf[64], buf[16];
	// strftime(tmbuf, sizeof tmbuf, "%Y-%m-%d %H:%M:%S", &result_tm);
	strftime(tmbuf, sizeof tmbuf, format, &result_tm);
	// snprintf(buf, sizeof buf, "%06d", (int)tv.tv_usec);
	sprintf(buf, "%06d", (int)tv.tv_usec);
	std::ostringstream node_oss;
	node_oss << tmbuf << "." << buf;
	return node_oss.str();
	
}

// std::string static
float
timeval_now_seconds_delta( const struct timeval & tv)
{
	
	struct timeval now_timeval, delta_timeval;
	//void timersub(struct timeval *a, struct timeval *b,
        //      struct timeval *res);
	gettimeofday(&now_timeval,NULL);
        timersub(&tv,&now_timeval,&delta_timeval);
	//char buf[64];
	//snprintf(buf, sizeof buf, "%d.%06d", (int)delta_timeval.tv_sec, (int)delta_timeval.tv_usec);
	//return buf;
	float retval = delta_timeval.tv_sec + delta_timeval.tv_usec * 1e-6;
	return retval;
}
