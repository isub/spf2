/*
 / _____)             _              | |
( (____  _____ ____ _| |_ _____  ____| |__
 \____ \| ___ |    (_   _) ___ |/ ___)  _ \
 _____) ) ____| | | || |_| ____( (___| | | |
(______/|_____)_|_|_| \__)_____)\____)_| |_|
  (C)2013 Semtech-Cycleo

Description:
    LoRa concentrator : Timer synchronization
        Provides synchronization between host and concentrator clocks

License: Revised BSD License, see LICENSE.TXT file include in the project
Maintainer: Michael Coracin
*/

/* -------------------------------------------------------------------------- */
/* --- DEPENDANCIES --------------------------------------------------------- */

#include <stdio.h>      /* printf, fprintf, snprintf, fopen, fputs */
#include <stdint.h>     /* C99 types */
#include <string.h>
#include <pthread.h>

#include "trace.h"
#include "timersync.h"
#include "sx1301ar_hal.h"
#include "sx1301ar_reg.h"
#include "sx1301ar_aux.h"

/* -------------------------------------------------------------------------- */
/* --- PRIVATE CONSTANTS & TYPES -------------------------------------------- */

/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/* --- PRIVATE VARIABLES (GLOBAL) ------------------------------------------- */

static pthread_mutex_t mx_counts = PTHREAD_MUTEX_INITIALIZER;
static uint32_t counts[SX1301AR_MAX_BOARD_NB] = { 0 };
static uint32_t offset = 0; /* timer offset between host and concentrator */

/* -------------------------------------------------------------------------- */
/* --- PRIVATE SHARED VARIABLES (GLOBAL) ------------------------------------ */
extern bool exit_sig;
extern bool quit_sig;
extern int board_nb;

/* -------------------------------------------------------------------------- */
/* --- PUBLIC FUNCTIONS DEFINITION ------------------------------------------ */

uint32_t sx1301ar_cnt2cnt(uint32_t count, int from, int to)
{
    uint32_t new;

    pthread_mutex_lock(&mx_counts);
    new = count + counts[to] - counts[from];
    pthread_mutex_unlock(&mx_counts);

    return new;
}

uint32_t sx1301ar_host2cnt(struct timeval *host, int board)
{
    uint32_t host_usec = host->tv_sec * 1000000 + host->tv_usec;
    return sx1301ar_cnt2cnt(host_usec - offset, 0, board);
}

/* ---------------------------------------------------------------------------------------------- */
/* --- THREAD 6: REGULARLAY MONITOR THE OFFSET BETWEEN UNIX CLOCK AND CONCENTRATOR CLOCK -------- */

void thread_timersync( void )
{
    struct timeval host;
    uint32_t news[SX1301AR_MAX_BOARD_NB] = { 0 };
    uint32_t count = 0;
    uint32_t previous;
    int board;

    while( !exit_sig && !quit_sig )
    {
        /* Get the last PPS latched time stamp for all boards */
        for (board = 0; board < board_nb; board++) {
            sx1301ar_get_trigcnt(board, &news[board]);
        }

        pthread_mutex_lock(&mx_counts);
        memcpy(&counts, &news, sizeof(counts));
        pthread_mutex_unlock(&mx_counts);

        /* Get current unix time */
        gettimeofday(&host, NULL);

        /* Get current concentrator counter value (1MHz) */
        sx1301ar_get_instcnt(0, &count);

        /* Compute offset between unix and concentrator timers, with microsecond precision */
        previous = offset;
        offset = (host.tv_sec * 1000000) + host.tv_usec - count;

        MSG_DEBUG( DEBUG_TIMERSYNC,
            "INFO: HOST/SX1301 time offset=(%.10uus) - drift=%.10uus\n",
            offset, offset - previous);

        /* delay next sync */
        /* If we consider a crystal oscillator precision of about 20ppm worst case, and a clock
            running at 1MHz, this would mean 1µs drift every 50000µs (10000000/20).
            As here the time precision is not critical, we should be able to cope with at least 1ms drift,
            which should occur after 50s (50000µs * 1000).
            Let's set the thread sleep to 1 minute for now */
        sx1301ar_wait_ms( 60000 );
    }
}
