/*
  ______                              _
 / _____)             _              | |
( (____  _____ ____ _| |_ _____  ____| |__
 \____ \| ___ |    (_   _) ___ |/ ___)  _ \
 _____) ) ____| | | || |_| ____( (___| | | |
(______/|_____)_|_|_| \__)_____)\____)_| |_|
  (C)2014 Semtech-Cycleo

Description:
    LoRa Gateway v2 packet forwarder

License: Revised BSD License, see LICENSE.TXT file include in the project
Maintainer: Michael Coracin
*/


/* -------------------------------------------------------------------------- */
/* --- DEPENDENCIES --------------------------------------------------------- */

/* Fix an issue between POSIX and C99 */
#if __STDC_VERSION__ >= 199901L
    #define _XOPEN_SOURCE 600
#else
    #define _XOPEN_SOURCE 500
#endif

#include <stdint.h>     /* C99 types */
#include <stdbool.h>    /* bool type */
#include <stdio.h>      /* NULL printf */
#include <stdlib.h>     /* EXIT_* */
#include <unistd.h>     /* getopt access */
#include <signal.h>     /* sigaction */
#include <string.h>     /* memset */
#include <time.h>       /* time clock_gettime strftime gmtime clock_nanosleep */
#include <sys/time.h>   /* timeval */
#include <errno.h>      /* error messages */
#include <fcntl.h>      /* open */
#include <termios.h>    /* tcflush */

#include <sys/socket.h> /* socket specific definitions */
#include <netinet/in.h> /* INET constants and stuff */
#include <arpa/inet.h>  /* IP address conversion stuff */
#include <netdb.h>      /* gai_strerror */

#include <pthread.h>

#include "sx1301ar_aux.h"
#include "sx1301ar_err.h"
#include "sx1301ar_hal.h"
#include "sx1301ar_gps.h"

#include "spi_linuxdev.h"
#include "i2c_linuxdev.h"

#include "parson.h"
#include "base64.h"
#include "timersync.h"
#include "jit.h"
#include "trace.h"

/* -------------------------------------------------------------------------- */
/* --- MACROS & CONSTANTS --------------------------------------------------- */

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

#define JSON_CONF_DEFAULT       "config.json"
#define JSON_LOCAL_CONF_DEFAULT "local.json"

#define JSON_SX1301AR_CONF_OBJ  "SX1301_array_conf"
#define JSON_GATEWAY_CONF_OBJ   "gateway_conf"
#define JSON_GPS_CONF_OBJ       "gps_conf"

/* Default values for Aartesys GW board */
#define LINUXDEV_PATH_DEFAULT   "/dev/spidev32766.0"
#define GPSTTY_PATH_DEFAULT     "/dev/ttyS4"
#define GPSI2C_PATH_DEFAULT     "/dev/i2c-1"

#define I2CADDR_GPS 0x42    /* I2C GPS device address */

#ifndef VERSION_STRING
    #define VERSION_STRING  "v0.0.?"
#endif

#define DEFAULT_KEEPALIVE   5   /* default time interval for downstream keep-alive packet */
#define DEFAULT_STAT        30  /* default time interval for statistics */
#define PUSH_TIMEOUT_MS     100
#define PULL_TIMEOUT_MS     200
#define FETCH_SLEEP_MS      1   /* nb of ms waited when a fetch return no packets */

#define PROTOCOL_VERSION    2

#define MIN_LORA_PREAMB 6 /* minimum LoRa preamble length for this application */
#define STD_LORA_PREAMB 8
#define MIN_FSK_PREAMB  3 /* minimum FSK preamble length for this application */
#define STD_FSK_PREAMB  5

#define STATUS_SIZE     300
#define TX_BUFF_SIZE    (840 * SX1301AR_MAX_PKT_NB)

#define N_SAT_MIN       2 /* minimum acceptable satellite number */
#define HDOP_MAX        10.0 /* maximum acceptable HDOP */
#define FAILED_SYNC_MAX 3 /* number of failed GPS sync that force a resync */
#define REF_VALIDITY    60 /* number of seconds a time reference is considered valid */

#define UPLINK_HISTORY_MAX 32

/* -------------------------------------------------------------------------- */
/* --- CUSTOM TYPES --------------------------------------------------------- */

typedef enum
{
    PKT_PUSH_DATA = 0,
    PKT_PUSH_ACK  = 1,
    PKT_PULL_DATA = 2,
    PKT_PULL_RESP = 3,
    PKT_PULL_ACK  = 4,
    PKT_TX_ACK    = 5
}
pkt_type_t;

typedef struct
{
    uint64_t gw_id; /*!> LoRa gateway 64b ID (eg. MAC address) */
    char serv_addr[64]; /*!> address of the server (host name or IPv4/IPv6) */
    char serv_port_up[8]; /*!> server port for upstream traffic */
    char serv_port_down[8]; /*!> server port for downstream traffic */
    int keepalive_time; /*!> send a PULL_DATA request every X seconds, negative = disabled */
    unsigned stat_interval; /*!> time interval (in sec) at which statistics are collected and displayed */
    struct timeval push_timeout_half; /*!> cut in half, critical for throughput */
    struct timeval pull_timeout; /*!> non critical for throughput */
    bool fwd_valid_pkt; /*!> packets with PAYLOAD CRC OK are forwarded */
    bool fwd_error_pkt; /*!> packets with PAYLOAD CRC ERROR are NOT forwarded */
    bool fwd_nocrc_pkt; /*!> packets with NO PAYLOAD CRC are NOT forwarded */
    uint32_t net_mac_h; /*!> Most Significant Nibble, network order */
    uint32_t net_mac_l; /*!> Least Significant Nibble, network order */
    uint32_t autoquit_threshold; /*!> enable auto-quit after a number of non-acknowledged PULL_DATA */
    uint32_t link_mote;
}
gw_conf_t;

typedef struct
{
    uint32_t rx_rcv; /*!> count packets received */
    uint32_t rx_ok; /*!> count packets received with PAYLOAD CRC OK */
    uint32_t rx_bad; /*!> count packets received with PAYLOAD CRC ERROR */
    uint32_t rx_nocrc ; /*!> count packets received with NO PAYLOAD CRC */
    uint32_t pkt_fwd; /*!> number of radio packet forwarded to the server */
    uint32_t network_byte; /*!> sum of UDP bytes sent for upstream traffic */
    uint32_t payload_byte; /*!> sum of radio payload bytes sent for upstream traffic */
    uint32_t dgram_sent; /*!> number of datagrams sent for upstream traffic */
    uint32_t ack_rcv; /*!> number of datagrams acknowledged for upstream traffic */
}
meas_up_t;

typedef struct
{
    uint32_t pull_sent; /*!> number of PULL requests sent for downstream traffic */
    uint32_t ack_rcv; /*!> number of PULL requests acknowledged for downstream traffic */
    uint32_t dgram_rcv; /*!> count PULL response packets received for downstream traffic */
    uint32_t network_byte; /*!> sum of UDP bytes sent for upstream traffic */
    uint32_t payload_byte; /*!> sum of radio payload bytes sent for upstream traffic */
    uint32_t tx_ok; /*!> count packets emitted successfully */
    uint32_t tx_fail; /*!> count packets were TX failed for other reasons */
    uint32_t tx_requested;
    uint32_t tx_rejected_collision_packet;
    uint32_t tx_rejected_too_late;
    uint32_t tx_rejected_too_early;
    uint32_t tx_rejected_collision_beacon;
}
meas_down_t;

typedef struct
{
    sx1301ar_coord_t coord; /*!> 3D geodesic coordinates */
    int n_sat; /*!> number of satellites of latest fix */
    float hdop; /*!> horizontal dilution of precision */
    int cnt_pps_lost; /*!> Counter of lost PPS */
    struct timespec utc; /*!> UTC time */
}
meas_gps_t;

typedef struct
{
    uint32_t link_mote_rcv_ok; /*!> count packets received from link testing mote with PAYLOAD CRC OK */
    uint16_t link_mote_fcnt_start; /*!> Sequence number of the first packet received from link testing mote */
    uint16_t link_mote_fcnt_now; /*!> Sequence number of the last packet received from link testing mote */
}
meas_lm_t;

typedef struct
{
    uint8_t token_l;
    uint8_t token_h;
    time_t time;
}
uplink_id_t;

typedef struct
{
    uplink_id_t uplinks[UPLINK_HISTORY_MAX];
    uint8_t size;
}
uplink_history_t;

/* -------------------------------------------------------------------------- */
/* --- GLOBAL VARIABLES ----------------------------------------------------- */

/* Signal handling variables */
int exit_sig = 0; /* 1 -> application terminates cleanly (shut down hardware, close open files, etc) */
int quit_sig = 0; /* 1 -> application terminates without shutting down the hardware */

/* Multi-Boards */
int board_nb = 1;

/* SPI interfaces */
int spi_context[SX1301AR_MAX_BOARD_NB]; /* contexts for linuxdev SPI driver (file descriptors) */

/* I2C interface */
int i2c_dev = -1;        /* context for linuxdev I2C driver (file descriptor) */

/* Network sockets */
static int sock_up; /* socket for upstream traffic */
static struct sockaddr_storage sa_up;
static socklen_t salen_up = sizeof(sa_up);
static int sock_down; /* socket for downstream traffic */
static struct sockaddr_storage sa_down;
static socklen_t salen_down = sizeof(sa_down);

/* Hardware access control and correction */
static pthread_mutex_t mx_concent[SX1301AR_MAX_BOARD_NB]; /* control access to the concentrator */

/* Measurements to establish statistics */
static meas_up_t meas_up;
static meas_down_t meas_dw;
static meas_gps_t meas_gps;
static meas_lm_t meas_lm;
static pthread_mutex_t mx_meas_up = PTHREAD_MUTEX_INITIALIZER; /* control access to the upstream measurements */
static pthread_mutex_t mx_meas_dw = PTHREAD_MUTEX_INITIALIZER; /* control access to the downstream measurements */
static pthread_mutex_t mx_meas_gps = PTHREAD_MUTEX_INITIALIZER; /* control access to the GPS measurements */
static pthread_mutex_t mx_meas_lm = PTHREAD_MUTEX_INITIALIZER; /* control access to the Link Mote measurements */

/* Synchronization time reference */
static sx1301ar_tref_t time_ref[SX1301AR_MAX_BOARD_NB];
static bool time_ref_valid = false;
static pthread_mutex_t mx_tref = PTHREAD_MUTEX_INITIALIZER; /* control access to the time reference */

/* GPS data */
static bool gps_coord_valid = false;

/* Status report */
static pthread_mutex_t mx_stat_rep = PTHREAD_MUTEX_INITIALIZER; /* control access to the status report */
static bool report_ready = false; /* true when there is a new report to send to the server */
static char status_report[STATUS_SIZE]; /* status report as a JSON object */

/* Uplink History */
static uplink_history_t up_history;
static pthread_mutex_t mx_up_hist = PTHREAD_MUTEX_INITIALIZER; /* control access to the uplink history */

/* Just In Time TX scheduling */
static jit_queue_t jit_queue;

/* TX limits */
uint32_t tx_freq_min[SX1301AR_BOARD_RFCHAIN_NB];
uint32_t tx_freq_max[SX1301AR_BOARD_RFCHAIN_NB];

/* SPI interfaces */
static const char spi1_path_default[] = LINUXDEV_PATH_DEFAULT;

/* -------------------------------------------------------------------------- */
/* --- SUBFUNCTIONS DECLARATION --------------------------------------------- */

/* Threads */
static void * thread_up( const void * arg );
static void * thread_up_ack( void );
static void * thread_down( const void * arg );
static void * thread_gps( const void * arg );
static void * thread_jit( void );

static void sig_handler( int sigio );

static void usage( void );

static int parse_sx1301ar_configuration( JSON_Object * brd_conf_obj, uint8_t brd );
static int parse_rfchain_configuration( JSON_Array * conf_rfchain_array, sx1301ar_board_cfg_t *cfg_brd );
static int parse_gateway_configuration( JSON_Object * gw_conf_obj, gw_conf_t * gwc );
static int parse_gps_configuration( JSON_Object * brd_conf_obj );
static int send_tx_ack( const gw_conf_t *gwc, uint8_t token_h, uint8_t token_l, jit_error_t error );

/* Wrappers around linuxdev SPI functions with explicit (global variables) SPI targets */
static int spi1_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status );
static int spi1_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status );
#if ( SX1301AR_MAX_BOARD_NB > 1 )
static int spi2_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status );
static int spi2_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status );
#endif
#if ( SX1301AR_MAX_BOARD_NB > 2 )
static int spi3_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status );
static int spi3_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status );
#endif
#if ( SX1301AR_MAX_BOARD_NB > 3 )
static int spi4_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status );
static int spi4_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status );
#endif
/* Wrappers around linuxdev I2C functions with explicit (global variables) I2C targets */
static int i2c_read( uint8_t device_addr, uint8_t reg_addr, uint8_t *data, uint16_t nb_bytes);
static int i2c_write( uint8_t device_addr, uint8_t *data, uint16_t nb_bytes );

/* Uplink history helpers */
static void history_init( void );
static int history_save( uint8_t token_l, uint8_t token_h );
static int history_check( uint8_t token_l, uint8_t token_h );
static void history_purge( void );

/* -------------------------------------------------------------------------- */
/* --- MAIN FUNCTION -------------------------------------------------------- */

int main( int argc, char ** argv )
{
    static struct sigaction sigact; /* SIGQUIT&SIGINT&SIGTERM signal handling */

    int i; /* loop and temporary variables */
    int x; /* return code for SX1301ar driver functions */

    /* GPS variables */
    bool enable_gps = false;
    bool configured_gps = false;
    const char gps_path_default[] = GPSTTY_PATH_DEFAULT;
    const char * gps_path = gps_path_default;
    int fd_gps = -1; /* file descriptor for the GPS TTY */
    struct termios ttyopt; /* serial port options */
    const char i2c_path_default[] = GPSI2C_PATH_DEFAULT;
    const char * i2c_path = i2c_path_default;
    int retry_init;

    /* Threads ID */
    pthread_t thrid_up;
    pthread_t thrid_up_ack;
    pthread_t thrid_down;
    pthread_t thrid_gps;
    pthread_t thrid_timersync;
    pthread_t thrid_jit;

    /* Gateway configuration variables */
    const char *hal_version;
    int16_t fpga_version;
    int16_t dsp_version;
    gw_conf_t gwc =
    {
        .gw_id = 0,
        .serv_addr = "127.0.0.1",
        .serv_port_up = "1780",
        .serv_port_down = "1782",
        .keepalive_time = DEFAULT_KEEPALIVE,
        .stat_interval = DEFAULT_STAT,
        .push_timeout_half = {0, (PUSH_TIMEOUT_MS * 500)},
        .pull_timeout = {0, (PULL_TIMEOUT_MS * 1000)},
        .fwd_valid_pkt = true,
        .fwd_error_pkt = false,
        .fwd_nocrc_pkt = false,
        .autoquit_threshold = 0
    };

    /* Configuration file */
    const char defaut_conf_fname[] = JSON_CONF_DEFAULT;
    const char * conf_fname = defaut_conf_fname; /* pointer to a string we won't touch */
    JSON_Value * root_val;
    JSON_Object * root = NULL;
    JSON_Object * conf = NULL;
    JSON_Array * conf_array = NULL;

    /* Network socket creation */
    struct addrinfo hints;
    struct addrinfo * result; /* store result of getaddrinfo */
    struct addrinfo * q; /* pointer to move into result data array */
    char host_name[64];
    char port_name[64];

    /* Variables to get local copies of measurements */
    meas_up_t cp_meas_up;
    meas_down_t cp_meas_dw;
    meas_gps_t cp_meas_gps;
    meas_lm_t cp_meas_lm;
    sx1301ar_tref_t cp_time_ref;

    /* Statistics variable */
    time_t t;
    char stat_timestamp[24];
    char boot_timestamp[24];
    float rx_ok_ratio;
    float rx_bad_ratio;
    float rx_nocrc_ratio;
    float up_ack_ratio;
    float dw_ack_ratio;
    struct timespec stat_delay;
    time_t ref_age;
    bool is_first = true;
    uint8_t gw_temperature_code;
    int16_t gw_temperature_celsius;

    /* Parse command line options */
    while( (i = getopt( argc, argv, "hyd:c:g:" )) != -1 )
    {
        switch( i )
        {
        case 'h':
            usage( );
            return EXIT_SUCCESS;
            break;

        case 'y': /* GPS is already configured */
            configured_gps = true;
            break;

        case 'g': /* -g <optional path>  use GPS receiver for synchronization */
            enable_gps = true;
            if( optarg != NULL )
            {
                gps_path = optarg;
            }
            break;

        case 'c':
            conf_fname = optarg;
            break;

        default:
            printf( "ERROR: argument parsing options, use -h option for help\n" );
            usage( );
            return EXIT_FAILURE;
        }
    }

    /* Start message */
    printf( "+++ Start of packet forwarder for 'SX1301 array' concentrator +++\n" );
    printf( "Version string: %s\n", VERSION_STRING );
    hal_version = sx1301ar_version_info( SX1301AR_BOARD_MASTER, NULL, NULL );
    printf( "SX1301 array library version string: %s\n", hal_version );

    /* In case of redirection, disable buffer */
    setbuf(stdout, NULL);


    if( enable_gps == true )
    {
        /* Init time reference (global val) */
        for (i = 0; i < (int)ARRAY_SIZE(time_ref); i++) {
            time_ref[i] = sx1301ar_init_tref( );
	}

        /* Open TTY device for GPS NMEA sentences reading */
        fd_gps = open( gps_path, O_RDWR | O_NOCTTY );
        if( fd_gps <= 0 )
        {
            printf( "ERROR: opening TTY port for GPS failed - %s (%s)\n", strerror(errno), gps_path );
            return EXIT_FAILURE;
        }

        /* Get TTY serial port parameters */
        x = tcgetattr( fd_gps, &ttyopt );
        if( x != 0 )
        {
            printf( "ERROR: failed to get GPS serial port parameters\n" );
            return EXIT_FAILURE;
        }

        /* Update TTY terminal parameters */
        ttyopt.c_cflag |= CLOCAL; /* local connection, no modem control */
        ttyopt.c_cflag |= CREAD; /* enable receiving characters */
        ttyopt.c_cflag |= CS8; /* 8 bit frames */
        ttyopt.c_cflag &= ~PARENB; /* no parity */
        ttyopt.c_cflag &= ~CSTOPB; /* one stop bit */
        ttyopt.c_iflag |= IGNPAR; /* ignore bytes with parity errors */
        ttyopt.c_iflag |= ICRNL; /* map CR to NL */
        ttyopt.c_iflag |= IGNCR; /* Ignore carriage return on input */
        ttyopt.c_lflag |= ICANON; /* enable canonical input */
        ttyopt.c_lflag &= ~ECHO; /* disable input being re-echoed as output */

        /* Set new TTY serial ports parameters */
        x = tcsetattr( fd_gps, TCSANOW, &ttyopt );
        if( x != 0 )
        {
            printf( "ERROR: failed to update GPS serial port parameters\n" );
            return EXIT_FAILURE;
        }

        /* Display message and flush */
        printf( "GPS enabled for synchronization using device %s\n", gps_path );
        tcflush( fd_gps, TCIOFLUSH );

        /* Open the I2C driver for GPS configuration (UBX) */
        if( configured_gps == false )
        {
            printf( "Opening the I2C device\n" );
            x = i2c_linuxdev_open( i2c_path, I2CADDR_GPS, &i2c_dev );
            if ( x != 0 )
            {
                printf( "ERRROR: failed to open I2C device (err=%i)\n", x );
                return EXIT_FAILURE;
            }
        }

        /* Initialize the PPS lost pulse counter */
        meas_gps.cnt_pps_lost = 0;
    }

    /* Display host endianness */
    #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        printf( "INFO: Little endian host\n" );
    #elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        printf( "INFO: Big endian host\n" );
    #else
        printf( "INFO: Host endianness unknown\n" );
    #endif

    /* Parse configuration file */
    if( access( conf_fname, R_OK ) == 0 )
    {
        printf( "INFO: found configuration file %s\n", conf_fname );
        root_val = json_parse_file_with_comments( conf_fname );
        root = json_value_get_object( root_val );
        if( root == NULL )
        {
            printf( "ERROR: %s id not a valid JSON file\n", conf_fname );
            return EXIT_FAILURE;
        }

        conf_array = json_object_get_array( root, JSON_SX1301AR_CONF_OBJ );
        if( conf_array != NULL)
        {
            /* Point to JSON object containing sx1301ar concentrator configuration for one board */
            board_nb = (int)json_array_get_count( conf_array );
            for( i = 0; i < board_nb; i++ )
            {
                if( i >= SX1301AR_MAX_BOARD_NB)
                {
                    printf( "ERROR: board %d not supported, skip it\n", i );
                    break;
                }
                conf = json_array_get_object( conf_array, i );
                if( conf == NULL )
                {
                    printf( "ERROR: configuration file does not contain a JSON object named %s\n", JSON_SX1301AR_CONF_OBJ );
                    return EXIT_FAILURE;
                }
                printf( "INFO: parsing concentrator parameters for board #%d\n", i );
                x = parse_sx1301ar_configuration( conf, i );
                if( x != 0 )
                {
                    printf( "ERROR: failed to parse configuration %s from JSON file\n", JSON_SX1301AR_CONF_OBJ );
                    return EXIT_FAILURE;
                }

                /* Initialize corresponding mutex */
                pthread_mutex_init( &mx_concent[i], NULL );
            }
        }

        /* Point to JSON object containing gateway configuration */
        conf = json_object_get_object( root, JSON_GATEWAY_CONF_OBJ );
        if( conf == NULL )
        {
            printf( "ERROR: configuration file does not contain a JSON object named %s\n", JSON_GATEWAY_CONF_OBJ );
            return EXIT_FAILURE;
        }
        printf( "INFO: parsing gateway parameters\n" );
        x = parse_gateway_configuration( conf, &gwc );
        if( x != 0 )
        {
            printf( "ERROR: failed to parse configuration %s from JSON file\n", JSON_GATEWAY_CONF_OBJ );
            return EXIT_FAILURE;
        }

        if( (enable_gps == true) && (configured_gps == false) )
        {
            /* Point to JSON object containing GPS configuration */
            conf = json_object_get_object( root, JSON_GPS_CONF_OBJ );
            printf( "INFO: parsing GPS parameters\n" );
            x = parse_gps_configuration( conf );
            if( x != 0 )
            {
                printf( "ERROR: failed to parse configuration %s from JSON file\n", JSON_GPS_CONF_OBJ );
                return EXIT_FAILURE;
            }

            /* Configure GPS */
            retry_init = 10; /* to cope with potential I2C transfer errors */
            while( retry_init + 1 )
            {
                retry_init -= 1;
                x = sx1301ar_init_gps();
                if( x == 0 )
                {
                    break;
                }
                else
                {
                    printf( "GPS: retrying GPS init (%d)...\n", retry_init );
                }
            }
            if( x != 0 )
            {
                printf( "ERROR: Failed to configure GPS; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
                return EXIT_FAILURE;
            }
        }

        /* Free JSON parsing structure */
        json_value_free( root_val );
    }
    else
    {
        printf( "ERROR: failed to find any configuration file named %s\n", conf_fname );
        return EXIT_FAILURE;
    }

    /* Prepare hints to open network sockets */
    memset( &hints, 0, sizeof hints );
    hints.ai_family = AF_UNSPEC; /* should handle IP v4 or v6 automatically */
    hints.ai_socktype = SOCK_DGRAM; /* we want UDP sockets */
    hints.ai_protocol = IPPROTO_UDP; /* we want UDP sockets */
    hints.ai_flags = AI_ADDRCONFIG; /* do not return IPv6 results if there is no IPv6 network connection, same with IPv4 */

    /* Look for server address w/ upstream port */
    x = getaddrinfo( gwc.serv_addr, gwc.serv_port_up, &hints, &result );
    if( x != 0 )
    {
        printf( "ERROR: [up] getaddrinfo on address %s (PORT %s) returned %s\n", gwc.serv_addr, gwc.serv_port_up, gai_strerror( x ) );
        return EXIT_FAILURE;
    }

    /* Try to open UDP socket for upstream traffic */
    for( q = result; q != NULL; q = q->ai_next )
    {
        sock_up = socket( q->ai_family, q->ai_socktype, q->ai_protocol );
        if( sock_up == -1 ) continue; /* try next field */
        else break; /* success, get out of loop */
    }
    if( q == NULL )
    {
        printf( "ERROR: [up] failed to open socket to any of server %s addresses (port %s)\n", gwc.serv_addr, gwc.serv_port_up );
        return EXIT_FAILURE;
    }
    else
    {
        getnameinfo( q->ai_addr, q->ai_addrlen, host_name, sizeof host_name, port_name, sizeof port_name, NI_NUMERICHOST );
        printf( "INFO: socket %i opened for upstream traffic, host: %s, port: %s\n", sock_up, host_name, port_name );
    }

    if (salen_up > q->ai_addrlen) {
        salen_up = q->ai_addrlen;
    }

    memcpy(&sa_up, q->ai_addr, salen_up);

    /* Free the result of getaddrinfo */
    freeaddrinfo( result );

    /* Look for server address w/ downstream port */
    x = getaddrinfo( gwc.serv_addr, gwc.serv_port_down, &hints, &result );
    if( x != 0 )
    {
        printf( "ERROR: [down] getaddrinfo on address %s (port %s) returned %s\n", gwc.serv_addr, gwc.serv_port_up, gai_strerror( x ) );
        return EXIT_FAILURE;
    }

    /* Try to open UDP socket for downstream traffic */
    for( q = result; q != NULL; q = q->ai_next )
    {
        sock_down = socket( q->ai_family, q->ai_socktype, q->ai_protocol );
        if( sock_down == -1 ) continue; /* try next field */
        else break; /* success, get out of loop */
    }
    if( q == NULL )
    {
        printf( "ERROR: [down] failed to open socket to any of server %s addresses (port %s)\n", gwc.serv_addr, gwc.serv_port_up );
        return EXIT_FAILURE;
    }
    else
    {
        getnameinfo( q->ai_addr, q->ai_addrlen, host_name, sizeof host_name, port_name, sizeof port_name, NI_NUMERICHOST );
        printf( "INFO: socket %i opened for downstream traffic, host: %s, port: %s\n", sock_up, host_name, port_name );
    }

    if (salen_down > q->ai_addrlen) {
        salen_down = q->ai_addrlen;
    }

    memcpy(&sa_down, q->ai_addr, salen_down);

    /* Free the result of getaddrinfo */
    freeaddrinfo( result );

    /* Put the MAC address in "network order" (for host endianness independence) */
    gwc.net_mac_h = htonl( (uint32_t)(0xFFFFFFFF & (gwc.gw_id >> 32)) );
    gwc.net_mac_l = htonl( (uint32_t)(0xFFFFFFFF &  gwc.gw_id  ) );

    /* Initialize uplink history */
    history_init( );

    /* Start the concentrator */
    x = sx1301ar_start( board_nb );
    if( x != 0 )
    {
        printf( "ERROR: sx1301ar_start failed ; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
        sx1301ar_stop( board_nb );
        return EXIT_FAILURE;
    }

    /* Spawn threads to manage upstream and downstream */
    x = pthread_create( &thrid_up, NULL, (void * (*)(void *))thread_up, (void *)&gwc );
    if( x != 0 )
    {
        printf( "ERROR: [main] impossible to create upstream thread\n" );
        return EXIT_FAILURE;
    }
    x = pthread_create( &thrid_up_ack, NULL, (void * (*)(void *))thread_up_ack, NULL );
    if( x != 0 )
    {
        printf( "ERROR: [main] impossible to create upstream ack thread\n" );
        return EXIT_FAILURE;
    }
    x = pthread_create( &thrid_down, NULL, (void * (*)(void *))thread_down, (void *)&gwc );
    if( x != 0 )
    {
        printf( "ERROR: [main] impossible to create downstream thread\n" );
        return EXIT_FAILURE;
    }
    x = pthread_create( &thrid_jit, NULL, (void * (*)(void *))thread_jit, NULL );
    if( x != 0 )
    {
        printf( "ERROR: [main] impossible to create just-in-time thread\n" );
        return EXIT_FAILURE;
    }
    i = pthread_create( &thrid_timersync, NULL, (void * (*)(void *))thread_timersync, NULL );
    if( x != 0 )
    {
        printf( "ERROR: [main] impossible to create Timer Sync thread\n" );
        return EXIT_FAILURE;
    }

    /* If enabled, spawn the thread in charge of GPS synchronization */
    if( enable_gps == true )
    {
        x = pthread_create( &thrid_gps, NULL, (void * (*)(void *))thread_gps, (void *)&fd_gps );
        if( x != 0 )
        {
            printf( "ERROR: [main] impossible to create GPS thread\n" );
            return EXIT_FAILURE;
        }
    }

    /* Configure statistics loop delay */
    stat_delay.tv_sec = gwc.stat_interval;
    stat_delay.tv_nsec = 0;

    /* Configure signal handling */
    sigemptyset( &sigact.sa_mask );
    sigact.sa_flags = 0;
    sigact.sa_handler = sig_handler;
    sigaction( SIGQUIT, &sigact, NULL );
    sigaction( SIGINT, &sigact, NULL );
    sigaction( SIGTERM, &sigact, NULL );

    /* Main loop; task: statistics collection */
    while( (quit_sig != 1) && (exit_sig != 1) )
    {
        /* Wait for next reporting interval */
        clock_nanosleep( CLOCK_MONOTONIC, 0, &stat_delay, NULL );

        /* Get timestamp for statistics */
        t = time( NULL );
        strftime( stat_timestamp, sizeof stat_timestamp, "%F %T %Z", gmtime( &t ) );

        /* Access upstream statistics, copy and reset them */
        pthread_mutex_lock( &mx_meas_up );
        cp_meas_up = meas_up;
        memset( (void *)&meas_up, 0, sizeof meas_up );
        pthread_mutex_unlock( &mx_meas_up );

        /* Access downstream statistics (global var), copy and reset them */
        pthread_mutex_lock( &mx_meas_dw );
        cp_meas_dw = meas_dw;
        memset( (void *)&meas_dw, 0, sizeof meas_dw );
        pthread_mutex_unlock( &mx_meas_dw );

        if( enable_gps == true )
        {
            /* Access GPS statistics (global var) and copy them */
            pthread_mutex_lock( &mx_meas_gps );
            cp_meas_gps = meas_gps;
            meas_gps.cnt_pps_lost = 0;
            pthread_mutex_unlock( &mx_meas_gps );

            /* Access time reference and copy it */
            pthread_mutex_lock( &mx_tref );
            cp_time_ref = time_ref[0];
            pthread_mutex_unlock( &mx_tref );
        }

        /* Access link mote statistics (global var), and copy them (no reset) */
        pthread_mutex_lock( &mx_meas_lm );
        cp_meas_lm = meas_lm;
        pthread_mutex_unlock( &mx_meas_lm );

        /* Calculate packet status ratio and acknowledgement ratio */
        if( cp_meas_up.rx_rcv > 0 )
        {
            rx_ok_ratio = (float)cp_meas_up.rx_ok / (float)cp_meas_up.rx_rcv;
            rx_bad_ratio = (float)cp_meas_up.rx_bad / (float)cp_meas_up.rx_rcv;
            rx_nocrc_ratio = (float)cp_meas_up.rx_nocrc / (float)cp_meas_up.rx_rcv;
        }
        else
        {
            rx_ok_ratio = 0.0;
            rx_bad_ratio = 0.0;
            rx_nocrc_ratio = 0.0;
        }
        if( cp_meas_up.dgram_sent > 0 )
        {
            up_ack_ratio = (float)cp_meas_up.ack_rcv / (float)cp_meas_up.dgram_sent;
        }
        else
        {
            up_ack_ratio = 0.0;
        }

        /* Calculate acknowledgement ratio */
        if( cp_meas_dw.pull_sent > 0 )
        {
            dw_ack_ratio = (float)cp_meas_dw.ack_rcv / (float)cp_meas_dw.pull_sent;
        }
        else
        {
            dw_ack_ratio = 0.0;
        }

        if( enable_gps == true )
        {
            /* WARNING: this loop is paced by stat_time, it could be a problem
                if stat_time is too long compared to REF_VALIDITY - TBC */
            pthread_mutex_lock( &mx_tref );
            ref_age = time( NULL ) - cp_time_ref.systime;
            time_ref_valid = (ref_age >= 0) && (ref_age < REF_VALIDITY);
            pthread_mutex_unlock( &mx_tref );
        }

        /* Get GW boot time */
        if( is_first == true )
        {
            if( enable_gps == true )
            {
                strftime( boot_timestamp, sizeof boot_timestamp, "%F %T %Z", gmtime(&(cp_meas_gps.utc.tv_sec)) );
            }
            else
            {
                strftime( boot_timestamp, sizeof boot_timestamp, "%F %T %Z", gmtime(&t) );
            }
            is_first = false;
        }

        /* Get gateway software versions */
        sx1301ar_version_info( SX1301AR_BOARD_MASTER, &fpga_version, &dsp_version );

        /* Display a report */
        printf( "\n##### %s #####\n", stat_timestamp );
        printf( "### [UPSTREAM] ###\n" );
        printf( "# RF packets received by concentrator: %u\n", cp_meas_up.rx_rcv );
        printf( "# CRC_OK: %.2f%%, CRC_FAIL: %.2f%%, NO_CRC: %.2f%%\n", 100.0 * rx_ok_ratio, 100.0 * rx_bad_ratio, 100.0 * rx_nocrc_ratio );
        printf( "# RF packets forwarded: %u (%u bytes)\n", cp_meas_up.pkt_fwd, cp_meas_up.payload_byte );
        printf( "# PUSH_DATA datagrams sent: %u (%u bytes)\n", cp_meas_up.dgram_sent, cp_meas_up.network_byte );
        printf( "# PUSH_DATA acknowledged: %.2f%%\n", 100.0 * up_ack_ratio );
        printf( "#\n" );
        printf( "### [DOWNSTREAM] ###\n" );
        printf( "# PULL_DATA sent: %u (%.2f%% acknowledged)\n", cp_meas_dw.pull_sent, 100.0 * dw_ack_ratio );
        printf( "# PULL_RESP(onse) datagrams received: %u (%u bytes)\n", cp_meas_dw.dgram_rcv, cp_meas_dw.network_byte );
        printf( "# RF packets sent to concentrator: %u (%u bytes)\n", cp_meas_dw.tx_ok + cp_meas_dw.tx_fail, cp_meas_dw.payload_byte );
        printf( "# TX errors: %u\n", cp_meas_dw.tx_fail );
        printf( "#\n" );
        printf("### [JIT] ###\n");
        jit_print_queue( &jit_queue, false, DEBUG_JIT );
        printf( "#\n" );
        if( enable_gps == true )
        {
            printf( "### [GPS] ###\n" );
            printf( "# %sVALID time reference (age %li seconds, %u updates), XTAL err %.3lf ppm\n", time_ref_valid ? "" : "IN", (long)ref_age, cp_time_ref.sync_cnt, (cp_time_ref.xtal_err - 1.0) * 1.0E6 );
            if( (time_ref_valid == true) && (gps_coord_valid==true) )
            {
                printf( "# GPS coordinates: latitude %.5f, longitude %.5f, altitude %i m\n", cp_meas_gps.coord.lat, cp_meas_gps.coord.lon, cp_meas_gps.coord.alt);
            }
            else
            {
                printf( "# no valid GPS coordinates available yet\n" );
            }
            printf( "# PPS pulses lost: %d\n", cp_meas_gps.cnt_pps_lost);
            printf( "#\n" );
        }
        if( gwc.link_mote != 0 )
        {
            printf( "### [LINK TESTING MOTE: %08X] ###\n", gwc.link_mote );
            printf( "# Packets received from link testing mote: %u\n", cp_meas_lm.link_mote_rcv_ok );
            if( cp_meas_lm.link_mote_rcv_ok != 0 )
            {
                printf( "# Sequence numbers: %u->%u (%u)\n", cp_meas_lm.link_mote_fcnt_start, cp_meas_lm.link_mote_fcnt_now, cp_meas_lm.link_mote_fcnt_now - cp_meas_lm.link_mote_fcnt_start + 1 );
            }
            printf( "#\n" );
        }
        sx1301ar_get_temperature( SX1301AR_BOARD_MASTER, &gw_temperature_code, &gw_temperature_celsius );
        printf( "### [GATEWAY] ###\n" );
        printf( "# Boot time: %s\n", boot_timestamp );
        printf( "# Temperature: %d C (code=%u)\n", gw_temperature_celsius, gw_temperature_code );
        printf( "#\n" );
        printf( "##### END #####\n" );

        /* generate a JSON report (will be sent to server by upstream thread) */
        pthread_mutex_lock(&mx_stat_rep);
        if ((enable_gps == true) && (gps_coord_valid == true))
        {
            snprintf(status_report, STATUS_SIZE, "\"stat\":{\"time\":\"%s\",\"boot\":\"%s\",\"lati\":%.5f,\"long\":%.5f,\"alti\":%i,\"rxnb\":%u,\"rxok\":%u,\"rxfw\":%u,\"ackr\":%.1f,\"dwnb\":%u,\"txnb\":%u,\"lmok\":%u,\"lmst\":%u,\"lmnw\":%u,\"pps\":%d,\"temp\":%d,\"fpga\":%u,\"dsp\":%d,\"hal\":\"%s\"}", stat_timestamp, boot_timestamp, cp_meas_gps.coord.lat, cp_meas_gps.coord.lon, cp_meas_gps.coord.alt, cp_meas_up.rx_rcv, cp_meas_up.rx_ok, cp_meas_up.pkt_fwd, 100.0 * up_ack_ratio, cp_meas_dw.dgram_rcv, cp_meas_dw.tx_ok, cp_meas_lm.link_mote_rcv_ok, cp_meas_lm.link_mote_fcnt_start, cp_meas_lm.link_mote_fcnt_now, cp_meas_gps.cnt_pps_lost, gw_temperature_celsius, fpga_version, dsp_version, hal_version);
        }
        else
        {
            snprintf(status_report, STATUS_SIZE, "\"stat\":{\"time\":\"%s\",\"boot\":\"%s\",\"rxnb\":%u,\"rxok\":%u,\"rxfw\":%u,\"ackr\":%.1f,\"dwnb\":%u,\"txnb\":%u,\"lmok\":%u,\"lmst\":%u,\"lmnw\":%u,\"temp\":%d,\"fpga\":%u,\"dsp\":%d,\"hal\":\"%s\"}", stat_timestamp, boot_timestamp, cp_meas_up.rx_rcv, cp_meas_up.rx_ok, cp_meas_up.pkt_fwd, 100.0 * up_ack_ratio, cp_meas_dw.dgram_rcv, cp_meas_dw.tx_ok, cp_meas_lm.link_mote_rcv_ok, cp_meas_lm.link_mote_fcnt_start, cp_meas_lm.link_mote_fcnt_now, gw_temperature_celsius, fpga_version, dsp_version, hal_version);
        }
        report_ready = true;
        pthread_mutex_unlock(&mx_stat_rep);
    }

    /* Wait for upstream thread to finish (1 fetch cycle max) */
    pthread_join( thrid_up, NULL );
    pthread_cancel( thrid_up_ack ); /* don't wait for uplink ack thread */
    pthread_cancel( thrid_down ); /* don't wait for downstream thread */
    pthread_cancel( thrid_timersync ); /* don't wait for timer sync thread */
    pthread_cancel( thrid_jit ); /* don't wait for jit thread */
    if( enable_gps == true )
    {
        pthread_cancel( thrid_gps ); /* don't wait for GPS thread */
    }

    /* If an exit signal was received, try to quit properly */
    if( exit_sig == 1 )
    {
        close(sock_up);
        close(sock_down);

        /* Stop the concentrator */
        x = sx1301ar_stop( board_nb );
        if( x != 0 )
        {
            printf( "ERROR: sx1301ar_stop failed ; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
            return EXIT_FAILURE;
        }
        printf( "INFO: concentrator stopped\n" );

        /* Close SPI link */
        for( i = 0; i < board_nb; i++ )
        {
            x = spi_linuxdev_close( spi_context[i] );
            if( x != 0 )
            {
                printf( "ERROR: closing SPI failed, returned %i\n", x );
                return EXIT_FAILURE;
            }
            printf( "INFO: SPI link %d closed\n", i );
        }

        /* Close GPS serial port */
        if( enable_gps == true )
        {
            x = close( fd_gps );
            if( x != 0 )
            {
                printf( "ERROR: closing GPS serial port failed\n" );
                return EXIT_FAILURE;
            }
            printf( "INFO: GPS serial port closed\n" );

            /* Close I2C link */
            if( configured_gps == false )
            {
                x = i2c_linuxdev_close( i2c_dev );
                if( x != 0 )
                {
                    printf( "ERROR: closing I2C failed, returned %i\n", x );
                    return EXIT_FAILURE;
                }
                printf( "INFO: I2C link closed\n" );
            }
        }
    }

    printf( "INFO: Exiting packet forwarder program\n" );
    exit( EXIT_SUCCESS );
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 1: RECEIVING PACKETS AND FORWARDING THEM ---------------------- */

static void * thread_up( const void * arg )
{
    int i, j; /* loop variables */
    int x; /* return code for SX1301ar driver functions */
    unsigned pkt_in_dgram; /* nb on LoRa packet in the current datagram */
    uint8_t board; /* board iterator */

    /* Gateway configuration (read only) */
    const gw_conf_t * gwc = arg;

    /* Allocate memory for packet fetching and processing */
    sx1301ar_rx_pkt_t rxbuf[SX1301AR_MAX_PKT_NB]; /* array to receive packet returned by fetch( ) */
    uint8_t nb_pkt; /* number of packets returned by fetch( ) */
    sx1301ar_rx_pkt_t * p; /* pointer to one RX packet structure */

    /* Local timestamp variables until we get accurate GPS time */
    struct timespec fetch_time;
    struct tm * t;

    /* Data buffers */
    uint8_t buff_up[TX_BUFF_SIZE]; /* buffer to compose the upstream packet */
    int buff_index;

    /* report management variable */
    bool send_report = false;

    /* Protocol variables */
    uint8_t token_h; /* random token for acknowledgement matching */
    uint8_t token_l; /* random token for acknowledgement matching */

    /* Statistics mote variables */
    uint32_t mote_addr = 0;
    bool mote_first = true;
    uint16_t mote_fcnt_start = 0;
    uint16_t mote_fcnt_now = 0;
    uint32_t mote_nb_rcv = 0;

    /* Variables to get local copies of measurements */
    sx1301ar_tref_t cp_time_ref;

    /* Set upstream socket RX timeout */
    i = setsockopt( sock_up, SOL_SOCKET, SO_RCVTIMEO, (void *)&(gwc->push_timeout_half), sizeof gwc->push_timeout_half );
    if( i != 0 )
    {
        printf( "ERROR: [up] setsockopt returned %s\n", strerror( errno ) );
        exit( EXIT_FAILURE );
    }

    /* Pre-fill the data buffer with fixed fields */
    buff_up[0] = PROTOCOL_VERSION;
    buff_up[3] = PKT_PUSH_DATA;
    *(uint32_t *)(buff_up + 4) = gwc->net_mac_h;
    *(uint32_t *)(buff_up + 8) = gwc->net_mac_l;

    board = -1;
    while( !exit_sig && !quit_sig )
    {
        /* Set next board or rotate */
        board++;
        if( board >= board_nb )
        {
            board = 0;
        }

        /* Fetch packets */
        pthread_mutex_lock( &mx_concent[board] );
        x = sx1301ar_fetch( board, rxbuf, ARRAY_SIZE( rxbuf ), &nb_pkt );
        pthread_mutex_unlock( &mx_concent[board] );
        if( x != 0 )
        {
            printf( "ERROR: sx1301ar_fetch failed ; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
            exit( EXIT_FAILURE );
        }

        /* check if there are status report to send */
        send_report = report_ready; /* copy the variable so it doesn't change mid-function */

        /* wait a short time if no packets found on all boards, nor status report */
        if( ( board == (board_nb-1)) && ( nb_pkt == 0 ) && ( send_report == false ) )
        {
            sx1301ar_wait_ms( FETCH_SLEEP_MS ); /* wait a short time if no packets */
            continue;
        }

        /* Start composing datagram with the header */
        token_h = (uint8_t)rand( ); /* random token */
        token_l = (uint8_t)rand( ); /* random token */
        buff_up[1] = token_h;
        buff_up[2] = token_l;
        buff_index = 12; /* 12-byte header */

        /* Start of JSON structure */
        memcpy( (void *)(buff_up + buff_index), (void *)"{\"rxpk\":[", 9 );
        buff_index += 9;

        /* Serialize LoRa packets metadata and payload */
        pkt_in_dgram = 0;
        for( i = 0; i < nb_pkt; ++i )
        {
            p = &rxbuf[i];

            /* Get mote information from current packet (addr, fcnt) */
            /* FHDR - DevAddr */
            mote_addr  = p->payload[1];
            mote_addr |= p->payload[2] << 8;
            mote_addr |= p->payload[3] << 16;
            mote_addr |= p->payload[4] << 24;
            /* FHDR - FCnt */
            mote_fcnt_now  = p->payload[6];
            mote_fcnt_now |= p->payload[7] << 8;

            /* Basic packet filtering */
            pthread_mutex_lock( &mx_meas_up );
            meas_up.rx_rcv += 1;
            switch( p->status )
            {
            case STAT_CRC_OK:
                meas_up.rx_ok += 1;
                printf( "\nINFO: [BRD%u] Received pkt from mote: %08X (fcnt=%u)\n", board, mote_addr, mote_fcnt_now );
                if( mote_addr == gwc->link_mote )
                {
                    /* Update statistics */
                    mote_nb_rcv += 1;
                    if( mote_first == true )
                    {
                        mote_fcnt_start = mote_fcnt_now;
                        mote_first = false;
                    }
                    /* meas_up is reset for each stat loop */
                    pthread_mutex_lock( &mx_meas_lm );
                    meas_lm.link_mote_rcv_ok = mote_nb_rcv;
                    meas_lm.link_mote_fcnt_start = mote_fcnt_start;
                    meas_lm.link_mote_fcnt_now = mote_fcnt_now;
                    pthread_mutex_unlock( &mx_meas_lm );
                }
                if( gwc->fwd_valid_pkt == false )
                {
                    pthread_mutex_unlock( &mx_meas_up );
                    continue; /* skip that packet */
                }
                break;

            case STAT_CRC_ERR:
                meas_up.rx_bad += 1;
                if( gwc->fwd_error_pkt == false )
                {
                    MSG_DEBUG( DEBUG_INFO, "\nINFO: [BRD%u] Skipping packet with CRC error\n", board );
                    pthread_mutex_unlock( &mx_meas_up );
                    continue; /* skip that packet */
                }
                break;

            case STAT_NO_CRC:
                meas_up.rx_nocrc += 1;
                if( gwc->fwd_nocrc_pkt == false )
                {
                    MSG_DEBUG( DEBUG_INFO, "\nINFO: [BRD%u] Skipping packet with no CRC\n", board );
                    pthread_mutex_unlock( &mx_meas_up );
                    continue; /* skip that packet */
                }
                break;

            default:
                printf( "WARNING: [BRD%u] [up] received packet with unknown status %u (size %u, modulation %u, BW %u, DR %u)\n", board, p->status, p->size, p->modulation, p->bandwidth, p->modrate );
                pthread_mutex_unlock( &mx_meas_up );
                continue; /* skip that packet */
            }
            meas_up.pkt_fwd += 1;
            meas_up.payload_byte += p->size;
            pthread_mutex_unlock( &mx_meas_up );

            /* Start of packet, add inter-packet separator if necessary */
            if( pkt_in_dgram == 0 )
            {
                buff_up[buff_index] = '{';
                ++buff_index;
            }
            else
            {
                buff_up[buff_index] = ',';
                buff_up[buff_index+1] = '{';
                buff_index += 2;
            }

            /* RAW timestamp, 8-17 useful chars */
            j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "\"tmst\":%u", sx1301ar_cnt2cnt(p->count_us, board, 0));
            if( j > 0 )
            {
                buff_index += j;
            }
            else
            {
                printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                exit( EXIT_FAILURE );
            }

            /* Packet RX time (system time based), 37 useful chars */
            if( time_ref_valid == true )
            {
                /* convert packet timestamp to UTC absolute time */
                pthread_mutex_lock( &mx_tref );
                j = sx1301ar_cnt2utc( time_ref[board], p->count_us, &fetch_time );
                pthread_mutex_unlock( &mx_tref );
                if( j == 0 )
                {
                    /* split the UNIX timestamp to its calendar components */
                    t = gmtime( &(fetch_time.tv_sec) );
                    j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"time\":\"%04i-%02i-%02iT%02i:%02i:%02i.%06liZ\"", (t->tm_year)+1900, (t->tm_mon)+1, t->tm_mday, t->tm_hour, t->tm_min, t->tm_sec, (fetch_time.tv_nsec)/1000 ); /* ISO 8601 format */
                    if( j > 0 )
                    {
                        buff_index += j;
                    }
                    else
                    {
                        printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                        exit( EXIT_FAILURE );
                    }
                }
                else
                {
                    printf( "ERROR: [up] failed to convert concentrator time to UTC\n" );
                }
            }

            /* Packet concentrator RF chain & RX frequency */
            j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"brd\":%u,\"aesk\":%u,\"freq\":%.6lf", board, board, ((double)p->freq_hz / 1e6) );
            if( j > 0 )
            {
                buff_index += j;
            }
            else
            {
                printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                exit( EXIT_FAILURE );
            }

            /* Packet status, 9-10 useful chars */
            switch( p->status )
            {
            case STAT_CRC_OK:
                memcpy( (void *)(buff_up + buff_index), (void *)",\"stat\":1", 10 );
                buff_index += 9;
                break;

            case STAT_CRC_ERR:
                memcpy( (void *)(buff_up + buff_index), (void *)",\"stat\":-1", 11 );
                buff_index += 10;
                break;

            case STAT_NO_CRC:
                memcpy( (void *)(buff_up + buff_index), (void *)",\"stat\":0", 10 );
                buff_index += 9;
                break;

            default:
                /* Invalid status case was already managed during packet filtering above */
                exit( EXIT_FAILURE );
            }

            /* Packet modulation, 13-14 useful chars */
            if( p->modulation == MOD_LORA )
            {
                memcpy( (void *)(buff_up + buff_index), (void *)",\"modu\":\"LORA\"", 14 );
                buff_index += 14;

                /* LoRa datarate & bandwidth, 18-19 useful chars */
                j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"datr\":\"SF%iBW%i\"", sx1301ar_sf_enum2nb( p->modrate ), (int)(sx1301ar_bw_enum2nb( p->bandwidth )/1000) );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    exit( EXIT_FAILURE );
                }

                /* Packet ECC coding rate, 13 useful chars */
                j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"codr\":\"%s\"", sx1301ar_cr_enum2str( p->coderate ) );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    exit( EXIT_FAILURE );
                }
            }
            else if( p->modulation == MOD_FSK )
            {
                memcpy( (void *)(buff_up + buff_index), (void *)",\"modu\":\"FSK\"", 13 );
                buff_index += 13;

                /* FSK datarate, 11-14 useful chars */
                j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"datr\":%u", p->modrate );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    exit( EXIT_FAILURE );
                }
            }
            else
            {
                printf( "ERROR: [up] received packet with unknown modulation\n" );
                exit( EXIT_FAILURE );
            }

            /* Access time reference and copy it */
            pthread_mutex_lock( &mx_tref );
            cp_time_ref = time_ref[board];
            pthread_mutex_unlock( &mx_tref );

            /* Packet payload size */
            j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"size\":%u", p->size );
            if( j > 0 )
            {
                buff_index += j;
            }
            else
            {
                printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                exit( EXIT_FAILURE );
            }

            /* Packet base64-encoded payload, 14-350 useful chars */
            memcpy( (void *)(buff_up + buff_index), (void *)",\"data\":\"", 9 );
            buff_index += 9;
            j = bin_to_b64( p->payload, p->size, (char *)(buff_up + buff_index), 341 ); /* 255 bytes = 340 chars in b64 + null char */
            if( j >= 0 )
            {
                buff_index += j;
            }
            else
            {
                printf( "ERROR: [up] bin_to_b64 failed line %u\n", (__LINE__ - 7) );
                exit( EXIT_FAILURE );
            }
            buff_up[buff_index] = '"';
            ++buff_index;

            /* Open rsig array */
            memcpy( (void *)(buff_up + buff_index), (void *)",\"rsig\":[", 9 );
            buff_index += 9;

            /* Open Antenna 0 object */
            if( p->rsig[0].is_valid == true )
            {
                memcpy( (void *)(buff_up + buff_index), (void *)"{\"ant\":0,", 9 );
                buff_index += 9;

                /* Fill Antenna 0 object */
                j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "\"chan\":%u,\"rssic\":%.0f,\"lsnr\":%.1f", p->rsig[0].chan, p->rsig[0].rssi_chan, p->rsig[0].snr );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    exit( EXIT_FAILURE );
                }

                /* Add fine timestamp info if available */
                if( (p->rsig[0].fine_received == true) && (time_ref_valid == true) )
                {
                    /* Add encrypted fine timestamp */
                    memcpy( (void *)(buff_up + buff_index), (void *)",\"etime\":\"", 10 );
                    buff_index += 10;
                    j = bin_to_b64( p->rsig[0].fine_tmst_enc, 16, (char *)(buff_up + buff_index), 26 );
                    if( j >= 0 )
                    {
                        buff_index += j;
                    }
                    else
                    {
                        printf( "ERROR: [up] bin_to_b64 failed line %u\n", (__LINE__ - 7) );
                        exit( EXIT_FAILURE );
                    }
                    buff_up[buff_index] = '"';
                    ++buff_index;

                    /* Add clear fine timestamp */
                    j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"ftime\":%d,\"ft2d\":%d,\"rfbsb\":%u,\"rs2s1\":%u", p->rsig[0].fine_tmst, p->rsig[0].fine_tmst_sb_delta_ns, p->rsig[0].ratio_energy_fb_sb, p->rsig[0].ratio_energy_sb2_sb1 );
                    if( j > 0 )
                    {
                        buff_index += j;
                    }
                    else
                    {
                        printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                        exit( EXIT_FAILURE );
                    }
                }

                /* Add extra signal info if available */
                if( p->rsig[0].sig_info_received == true )
                {
                    /* Compensation of the Gateway Xtal error */
                    p->rsig[0].freq_offset = p->rsig[0].freq_offset - (int16_t)((1.0 - cp_time_ref.xtal_hs_err)*p->freq_hz);

                    /* Add clear fine timestamp, and frequency offset */
                    j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"rssis\":%.0f,\"rssisd\":%u,\"foff\":%d", p->rsig[0].rssi_sig, p->rsig[0].rssi_sig_std, p->rsig[0].freq_offset );
                    if( j > 0 )
                    {
                        buff_index += j;
                    }
                    else
                    {
                        printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                        exit( EXIT_FAILURE );
                    }
                }

                /* Close Antenna 0 object */
                buff_up[buff_index] = '}';
                buff_index += 1;
            }

            if( p->rsig[1].is_valid == true )
            {
                /* Open Antenna 1 object */
                if( p->rsig[0].is_valid == true )
                {
                    buff_up[buff_index] = ','; /* add separator if there was a previous object */
                    buff_index += 1;
                }
                memcpy( (void *)(buff_up + buff_index), (void *)"{\"ant\":1,", 9 );
                buff_index += 9;

                /* Fill Antenna 1 object */
                j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "\"chan\":%u,\"rssic\":%.0f,\"lsnr\":%.1f", p->rsig[1].chan, p->rsig[1].rssi_chan, p->rsig[1].snr );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    exit( EXIT_FAILURE );
                }

                /* Add fine timestamp info if available */
                if( (p->rsig[1].fine_received == true) && (time_ref_valid == true) )
                {
                    /* Add encrypted fine timestamp */
                    memcpy( (void *)(buff_up + buff_index), (void *)",\"etime\":\"", 10 );
                    buff_index += 10;
                    j = bin_to_b64( p->rsig[1].fine_tmst_enc, 16, (char *)(buff_up + buff_index), 26 );
                    if( j >= 0 )
                    {
                        buff_index += j;
                    }
                    else
                    {
                        printf( "ERROR: [up] bin_to_b64 failed line %u\n", (__LINE__ - 7) );
                        exit( EXIT_FAILURE );
                    }
                    buff_up[buff_index] = '"';
                    ++buff_index;

                    /* Add clear fine timestamp */
                    j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"ftime\":%d,\"ft2d\":%d,\"rfbsb\":%u,\"rs2s1\":%u", p->rsig[1].fine_tmst, p->rsig[1].fine_tmst_sb_delta_ns, p->rsig[1].ratio_energy_fb_sb, p->rsig[1].ratio_energy_sb2_sb1 );
                    if( j > 0 )
                    {
                        buff_index += j;
                    }
                    else
                    {
                        printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                        exit( EXIT_FAILURE );
                    }
                }

                /* Add extra signal info if available */
                if( p->rsig[1].sig_info_received == true )
                {
                    /* Compensation of the Gateway Xtal error */
                    p->rsig[1].freq_offset = p->rsig[1].freq_offset - (int16_t)((1.0 - cp_time_ref.xtal_hs_err)*p->freq_hz);

                    /* Add clear fine timestamp, and frequency offset */
                    j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, ",\"rssis\":%.0f,\"rssisd\":%u,\"foff\":%d", p->rsig[1].rssi_sig, p->rsig[1].rssi_sig_std, p->rsig[1].freq_offset );
                    if( j > 0 )
                    {
                        buff_index += j;
                    }
                    else
                    {
                        printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                        exit( EXIT_FAILURE );
                    }
                }

                /* Close Antenna 1 object */
                buff_up[buff_index] = '}';
                ++buff_index;
            }

            /* Close fine timestamp array */
            buff_up[buff_index] = ']';
            ++buff_index;

            /* End of packet serialization */
            buff_up[buff_index] = '}';
            ++buff_index;
            ++pkt_in_dgram;
        }

        /* Restart fetch sequence without sending empty JSON if all packets have been filtered out */
        if( pkt_in_dgram == 0 )
        {
            if( send_report == true )
            {
                /* need to clean up the beginning of the payload */
                buff_index -= 8; /* removes "rxpk":[ */
            }
            else
            {
                /* all packet have been filtered out and no report, restart loop */
                continue;
            }
        }
        else
        {
            /* End of packet array */
            buff_up[buff_index] = ']';
            ++buff_index;

            /* add separator if needed */
            if( send_report == true )
            {
                buff_up[buff_index] = ',';
                ++buff_index;
            }
        }

        /* add status report if a new one is available */
        if( send_report == true )
        {
            pthread_mutex_lock( &mx_stat_rep );
            report_ready = false;
            j = snprintf( (char *)(buff_up + buff_index), TX_BUFF_SIZE-buff_index, "%s", status_report );
            pthread_mutex_unlock( &mx_stat_rep );
            if( j > 0 )
            {
                buff_index += j;
            }
            else
            {
                printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 8) );
                exit( EXIT_FAILURE );
            }
        }

        /* End of JSON datagram payload */
        buff_up[buff_index] = '}';
        ++buff_index;
        buff_up[buff_index] = 0; /* add string terminator, for safety */

        printf( "\n[%llu] JSON up [%u] [%02X%02X]: %s\n", sx1301ar_timestamp(), pkt_in_dgram, buff_up[1], buff_up[2], (char *)(buff_up + 12) ); /* DEBUG: display JSON payload */

        /* Send datagram to server */
        j = sendto( sock_up, (void *)buff_up, buff_index, 0, (struct sockaddr *)&sa_up, salen_up);
        if( j == -1 )
        {
            printf( "ERROR: failed to send uplink packet - %s (%d)\n", strerror(errno), errno);
            printf( "DEBUG: sock:%d; buff:%p; index:%d; sa:%p; salen:%u;\n", sock_up, buff_up, buff_index, &sa_up, salen_up);
            continue;
        }

        pthread_mutex_lock( &mx_meas_up );
        meas_up.dgram_sent += 1;
        meas_up.network_byte += buff_index;

        /* Save history for ACK check */
        history_purge( );
        history_save( token_l, token_h );

        pthread_mutex_unlock( &mx_meas_up );
    }
    printf( "\nINFO: End of upstream thread\n" );
    return NULL;
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 2: RECEIVING UPLINK ACKNOLEDGES FROM SERVER-------------------- */

static void * thread_up_ack( void )
{
    int x;

    /* Data buffers */
    uint8_t buff_ack[32]; /* buffer to receive acknowledges */

    while( !exit_sig && !quit_sig )
    {
        x = recvfrom( sock_up, (void *)buff_ack, sizeof buff_ack, 0, NULL, NULL);
        if( x == -1 )
        {
            if( errno == EAGAIN )
            {
                continue; /* timeout */
            }
            else
            { /* server connection error */
                printf( "ERROR: failed to connect to server to get uplink acknowledge (%s)\n", strerror(errno) );
            }
            sx1301ar_wait_us( 10000 ); /* wait a short time if no data on socket */
        }
        else if( (x < 4) || (buff_ack[0] != PROTOCOL_VERSION) || (buff_ack[3] != PKT_PUSH_ACK) )
        {
            printf( "WARNING: [up] ignored invalid non-ACL packet (id=0x%02X, size=%u)\n", buff_ack[3], x );
            continue;
        }
        else
        {
            x = history_check( buff_ack[2], buff_ack[1] );
            if( x == 0 )
            {
                printf( "INFO: [up] ACK received [%02X%02X] :)\n", buff_ack[1], buff_ack[2] ); /* too verbose */
                pthread_mutex_lock( &mx_meas_up );
                meas_up.ack_rcv += 1;
                pthread_mutex_unlock( &mx_meas_up );
            }
            else
            {
                printf( "WARNING: [up] received unknown ACK packet [%02X%02X]\n", buff_ack[1], buff_ack[2] );
            }
        }
    }
    printf( "\nINFO: End of upstream ack thread\n" );
    return NULL;
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 3: POLLING SERVER AND EMITTING PACKETS ------------------------ */

static void * thread_down( const void * arg )
{
    int i; /* loop variables */

    /* Gateway configuration (read only) */
    const gw_conf_t * gwc = arg;

    /* Configuration and metadata for an outbound packet */
    sx1301ar_tx_pkt_t p;
    bool sent_immediate = false; /* option to sent the packet immediately */

    /* Local timekeeping variables */
    time_t now; /* current time, with second accuracy */
    time_t requ_time; /* time of the pull request, low-res OK */

    /* Data buffers */
    uint8_t buff_down[1000]; /* buffer to receive downstream packets */
    uint8_t buff_req[12]; /* buffer to compose pull requests */
    int msg_len;

    /* Protocol variables */
    uint8_t token_h; /* random token for acknowledgement matching */
    uint8_t token_l; /* random token for acknowledgement matching */
    bool req_ack = false; /* keep track of whether PULL_DATA was acknowledged or not */

    /* JSON parsing variables */
    JSON_Value * root_val = NULL;
    JSON_Object * txpk_obj = NULL;
    JSON_Value * val = NULL; /* needed to detect the absence of some fields */
    const char * str; /* pointer to sub-strings in the JSON data */
    short x0, x1;

    /* Auto-quit variable */
    uint32_t autoquit_cnt = 0; /* count the number of PULL_DATA sent since the latest PULL_ACK */

    /* Just In Time downlink */
    uint8_t board;
    struct timeval host_tm;
    jit_error_t jit_result = JIT_ERROR_OK;
    jit_pkt_type_t downlink_type;

    /* Set downstream socket RX timeout */
    i = setsockopt( sock_down, SOL_SOCKET, SO_RCVTIMEO, (void *)&(gwc->pull_timeout), sizeof gwc->pull_timeout );
    if( i != 0 )
    {
        printf( "ERROR: [down] setsockopt returned %s\n", strerror( errno) );
        exit( EXIT_FAILURE );
    }

    /* Pre-fill the pull request buffer with fixed fields */
    buff_req[0] = PROTOCOL_VERSION;
    buff_req[3] = PKT_PULL_DATA;
    *(uint32_t *)(buff_req + 4) = gwc->net_mac_h;
    *(uint32_t *)(buff_req + 8) = gwc->net_mac_l;

    /* JIT queue initialization */
    jit_queue_init( &jit_queue );

    while( !exit_sig && !quit_sig )
    {
        /* Auto-quit if the threshold is crossed */
        if( (gwc->autoquit_threshold > 0) && (autoquit_cnt >= gwc->autoquit_threshold) )
        {
            exit_sig = true;
            printf( "INFO: [down] the last %u PULL_DATA were not ACKed, exiting application\n", gwc->autoquit_threshold );
            break;
        }

        /* Generate random token for request */
        token_h = (uint8_t)rand( ); /* random token */
        token_l = (uint8_t)rand( ); /* random token */
        buff_req[1] = token_h;
        buff_req[2] = token_l;

        /* Send PULL request and record time */
        sendto( sock_down, (void *)buff_req, sizeof buff_req, 0, (struct sockaddr *)&sa_down, salen_down);
        pthread_mutex_lock( &mx_meas_dw );
        meas_dw.pull_sent += 1;
        pthread_mutex_unlock( &mx_meas_dw );
        req_ack = false;
        autoquit_cnt++;

        /* Listen to packets and process them until a new PULL request must be sent */
        for( time( &requ_time ); (int)difftime( now, requ_time ) < gwc->keepalive_time; time( &now ) )
        {
            /* Try to receive a datagram */
            msg_len = recvfrom( sock_down, (void *)buff_down, (sizeof buff_down)-1, 0, NULL, NULL);

            /* If no network message was received, got back to listening sock_down socket */
            if( msg_len == -1 )
            {
                //printf( "WARNING: [down] recv returned %s\n", strerror( errno ) ); /* too verbose */
                continue;
            }

            /* If the datagram does not respect protocol, just ignore it */
            if( (msg_len < 4) || (buff_down[0] != PROTOCOL_VERSION) || ((buff_down[3] != PKT_PULL_RESP) && (buff_down[3] != PKT_PULL_ACK)) )
            {
                printf( "WARNING: [down] ignoring invalid packet\n" );
                continue;
            }

            /* If the datagram is an ACK, check token */
            if( buff_down[3] == PKT_PULL_ACK )
            {
                if( (buff_down[1] == token_h) && (buff_down[2] == token_l) )
                {
                    if( req_ack )
                    {
                        printf( "INFO: [down] duplicate ACK received :)\n" );
                    }
                    else
                    { /* If that packet was not already acknowledged */
                        req_ack = true;
                        autoquit_cnt = 0;
                        pthread_mutex_lock( &mx_meas_dw );
                        meas_dw.ack_rcv += 1;
                        pthread_mutex_unlock( &mx_meas_dw );
                        printf( "INFO: [down] ACK received :)\n" ); /* very verbose */
                    }
                }
                else
                { /* Out-of-sync token */
                    printf( "INFO: [down] received out-of-sync ACK\n" );
                }
                continue;
            }

            /* The datagram is a PULL_RESP */
            buff_down[msg_len] = 0; /* add string terminator, just to be safe */
            printf( "INFO: [down] PULL_RESP received :)\n" ); /* very verbose */
            printf( "\n[%llu] JSON down: %s\n", sx1301ar_timestamp(), (char *)(buff_down + 4) ); /* DEBUG: display JSON payload */

            /* Initialize TX struct and try to parse JSON */
            p = sx1301ar_init_tx_pkt( );
            root_val = json_parse_string_with_comments( (const char *)(buff_down + 4) ); /* JSON offset */
            if( root_val == NULL )
            {
                printf( "WARNING: [down] invalid JSON, TX aborted\n" );
                continue;
            }

            /* Look for JSON sub-object 'txpk' */
            txpk_obj = json_object_get_object( json_value_get_object( root_val ), "txpk" );
            if( txpk_obj == NULL )
            {
                printf( "WARNING: [down] no \"txpk\" object in JSON, TX aborted\n" );
                json_value_free( root_val );
                continue;
            }

            /* Parse "immediate" tag, or target timestamp, or UTC time to be converted by GPS (mandatory) */
            i = json_object_get_boolean( txpk_obj, "imme" ); /* can be 1 if true, 0 if false, or -1 if not a JSON boolean */
            if( i == 1 )
            {
                /* TX procedure: send immediately */
                sent_immediate = true;
                downlink_type = JIT_PKT_TYPE_DOWNLINK_CLASS_C;
                printf( "INFO: [down] a packet will be sent in \"immediate\" mode\n" );
            }
            else
            {
                sent_immediate = false;
                val = json_object_get_value( txpk_obj, "tmst" );
                if( val != NULL )
                {
                    /* TX procedure: send on timestamp value */
                    p.count_us = (uint32_t)json_value_get_number( val );
                    printf( "INFO: [down] a packet will be sent on timestamp value %u\n", p.count_us );

                    /* Concentrator timestamp is given, we consider it is a Class A downlink */
                    downlink_type = JIT_PKT_TYPE_DOWNLINK_CLASS_A;
                }
                else
                {
                    printf( "WARNING: [down] only \"immediate\" and \"timestamp\" modes supported, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
            }

            /* Parse "No CRC" flag (optional field) */
            val = json_object_get_value( txpk_obj,"ncrc" );
            if( val != NULL )
            {
                p.no_crc = (bool)json_value_get_boolean( val );
            }

            /* Parse target frequency (mandatory) */
            val = json_object_get_value( txpk_obj,"freq" );
            if( val == NULL )
            {
                printf( "WARNING: [down] no mandatory \"txpk.freq\" object in JSON, TX aborted\n" );
                json_value_free( root_val );
                continue;
            }
            p.freq_hz = (uint32_t)(1e6 * json_value_get_number( val ) );

            /* Parse RF chain / Antenna used for TX (mandatory) */
            val = json_object_get_value( txpk_obj,"ant" );
            if( val == NULL )
            {
                val = json_object_get_value( txpk_obj,"rfch" );
                if( val == NULL )
                {
                    printf( "WARNING: [down] no mandatory \"txpk.ant\" or \"txpk.rfch\" object in JSON, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
            }
            p.rf_chain = (uint8_t)json_value_get_number( val );

            /* Parse target board to be used for TX (optional) */
            board = SX1301AR_BOARD_MASTER;
            val = json_object_get_value( txpk_obj,"brd" );
            if( val != NULL )
            {
                board = (uint8_t)json_value_get_number( val );
                p.count_us = sx1301ar_cnt2cnt(p.count_us, 0, board);
            }

            /* Parse TX power (optional field) */
            val = json_object_get_value( txpk_obj,"powe" );
            if( val != NULL )
            {
                p.rf_power = (int8_t)json_value_get_number( val );
            }

            /* Parse modulation (mandatory) */
            str = json_object_get_string( txpk_obj, "modu" );
            if( str == NULL )
            {
                printf( "WARNING: [down] no mandatory \"txpk.modu\" object in JSON, TX aborted\n" );
                json_value_free( root_val );
                continue;
            }
            if( strcmp( str, "LORA" ) == 0 )
            {
                /* LoRa modulation */
                p.modulation = MOD_LORA;

                /* Parse LoRa spreading-factor and modulation bandwidth (mandatory) */
                str = json_object_get_string( txpk_obj, "datr" );
                if( str == NULL )
                {
                    printf( "WARNING: [down] no mandatory \"txpk.datr\" object in JSON, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
                i = sscanf( str, "SF%2hdBW%3hd", &x0, &x1 );
                if( i != 2 )
                {
                    printf( "WARNING: [down] format error in \"txpk.datr\", TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
                p.modrate = sx1301ar_sf_nb2enum( x0 );
                if( p.modrate == MR_UNDEFINED )
                {
                    printf( "WARNING: [down] format error in \"txpk.datr\", invalid SF, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
                p.bandwidth = sx1301ar_bw_nb2enum( x1 * 1000 );
                if( p.bandwidth == BW_UNDEFINED )
                {
                    printf( "WARNING: [down] format error in \"txpk.datr\", invalid BW, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }

                /* Parse ECC coding rate (optional field) */
                str = json_object_get_string( txpk_obj, "codr" );
                if( str == NULL )
                {
                    printf( "WARNING: [down] no mandatory \"txpk.codr\" object in json, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
                if(      strcmp( str, "4/5" ) == 0 ) p.coderate = CR_4_5;
                else if( strcmp( str, "4/6" ) == 0 ) p.coderate = CR_4_6;
                else if( strcmp( str, "2/3" ) == 0 ) p.coderate = CR_4_6;
                else if( strcmp( str, "4/7" ) == 0 ) p.coderate = CR_4_7;
                else if( strcmp( str, "4/8" ) == 0 ) p.coderate = CR_4_8;
                else if( strcmp( str, "1/2" ) == 0 ) p.coderate = CR_4_8;
                else {
                    printf( "WARNING: [down] format error in \"txpk.codr\", TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }

                /* Parse signal polarity switch(optional field) */
                val = json_object_get_value( txpk_obj,"ipol" );
                if( val != NULL )
                {
                    p.invert_pol = (bool)json_value_get_boolean( val );
                }

                /* Parse LoRa preamble length (optional field, optimum min value enforced) */
                val = json_object_get_value( txpk_obj,"prea" );
                if( val != NULL )
                {
                    i = (int)json_value_get_number( val );
                    if( i >= MIN_LORA_PREAMB )
                    {
                        p.preamble = (uint16_t)i;
                    }
                    else
                    {
                        p.preamble = (uint16_t)MIN_LORA_PREAMB;
                    }
                }
                else
                {
                    p.preamble = (uint16_t)STD_LORA_PREAMB;
                }

            }
            else if( strcmp( str, "FSK" ) == 0 )
            {
                /* FSK modulation */
                p.modulation = MOD_FSK;

                /* Parse FSK bitrate (mandatory) */
                val = json_object_get_value( txpk_obj, "datr" );
                if( val == NULL )
                {
                    printf( "WARNING: [down] no mandatory \"txpk.datr\" object in JSON, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
                p.modrate = (uint32_t)json_value_get_number( val );

                /* Parse frequency deviation (mandatory) */
                val = json_object_get_value( txpk_obj, "fdev" );
                if( val == NULL )
                {
                    printf( "WARNING: [down] no mandatory \"txpk.fdev\" object in JSON, TX aborted\n" );
                    json_value_free( root_val );
                    continue;
                }
                p.f_dev = (uint8_t)json_value_get_number( val );

                /* Parse FSK preamble length (optional field, optimum min value enforced) */
                val = json_object_get_value( txpk_obj, "prea" );
                if( val != NULL )
                {
                    i = (int)json_value_get_number( val );
                    if( i >= MIN_FSK_PREAMB )
                    {
                        p.preamble = (uint16_t)i;
                    }
                    else
                    {
                        p.preamble = (uint16_t)MIN_FSK_PREAMB;
                    }
                }
                else
                {
                    p.preamble = (uint16_t)STD_FSK_PREAMB;
                }

            }
            else
            {
                printf( "WARNING: [down] invalid modulation in \"txpk.modu\", TX aborted\n" );
                json_value_free( root_val );
                continue;
            }

            /* Parse payload length (mandatory) */
            val = json_object_get_value( txpk_obj,"size" );
            if( val == NULL )
            {
                printf( "WARNING: [down] no mandatory \"txpk.size\" object in JSON, TX aborted\n" );
                json_value_free( root_val );
                continue;
            }
            p.size = (uint16_t)json_value_get_number( val );

            /* Parse payload data (mandatory) */
            str = json_object_get_string( txpk_obj, "data" );
            if( str == NULL )
            {
                printf( "WARNING: [down] no mandatory \"txpk.data\" object in JSON, TX aborted\n" );
                json_value_free( root_val );
                continue;
            }
            i = b64_to_bin( str, strlen( str ), p.payload, sizeof p.payload );
            if( i != p.size )
            {
                printf( "WARNING: [down] mismatch between .size and .data size once converter to binary\n" );
            }

            /* Free the JSON parse tree from memory */
            json_value_free( root_val );

            /* Select TX mode */
            if( sent_immediate )
            {
                p.tx_mode = TX_IMMEDIATE;
            }
            else
            {
                p.tx_mode = TX_TIMESTAMPED;
            }

            /* check TX parameter before trying to queue packet */
            jit_result = JIT_ERROR_OK;
            if( ((tx_freq_min[p.rf_chain] != 0) && (p.freq_hz < tx_freq_min[p.rf_chain])) || ((tx_freq_max[p.rf_chain] != 0) && (p.freq_hz > tx_freq_max[p.rf_chain])) )
            {
                jit_result = JIT_ERROR_TX_FREQ;
                printf( "ERROR: Packet REJECTED, unsupported frequency - %u (min:%u,max:%u)\n", p.freq_hz, tx_freq_min[p.rf_chain], tx_freq_max[p.rf_chain] );
            }
            /* No check on TX power has the HAL will choose the closest supported power */

            /* insert packet to be sent into JIT queue */
            if( jit_result == JIT_ERROR_OK )
            {
                gettimeofday( &host_tm, NULL );
                jit_result = jit_enqueue( &jit_queue, &host_tm, board, &p, downlink_type );
                if( jit_result != JIT_ERROR_OK )
                {
                    printf( "ERROR: Packet REJECTED (jit error=%d)\n", jit_result );
                }
                pthread_mutex_lock( &mx_meas_dw );
                meas_dw.tx_requested += 1;
                pthread_mutex_unlock( &mx_meas_dw );
            }

            /* Send acknowledge datagram to server */
            i = send_tx_ack( gwc, buff_down[1], buff_down[2], jit_result );
            if( i < 0 )
            {
                printf( "WARNING: Failed to send tx_ack datagram to server\n" );
            }
        }
    }
    printf( "\nINFO: End of downstream thread\n" );
    return NULL;
}

/* -------------------------------------------------------------------------- */
/* --- THREAD X: CHECKING PACKETS TO BE SENT FROM JIT QUEUE AND SEND THEM --- */

static void * thread_jit(void)
{
    int result = 0;
    sx1301ar_tx_pkt_t pkt;
    int pkt_index = -1;
    struct timeval host_tm;
    jit_error_t jit_result;
    jit_pkt_type_t pkt_type;
    sx1301ar_tstat_t tx_status;
    uint8_t board;

    while( !exit_sig && !quit_sig )
    {
        sx1301ar_wait_ms( 10 );

        /* transfer data and metadata to the concentrator, and schedule TX */
        gettimeofday( &host_tm, NULL );
        jit_result = jit_peek( &jit_queue, &host_tm, &pkt_index );
        if( jit_result == JIT_ERROR_OK )
        {
            if( pkt_index > -1 )
            {
                jit_result = jit_dequeue( &jit_queue, pkt_index, &board, &pkt, &pkt_type );
                if( jit_result == JIT_ERROR_OK )
                {
                    /* check if concentrator is free for sending new packet */
                    result = sx1301ar_tx_status( board, &tx_status );
                    if( result != 0 )
                    {
                        printf( "WARNING: [jit] lgw_status failed - %s\n", sx1301ar_err_message(sx1301ar_errno) );
                    }
                    else
                    {
                        if( tx_status == TX_EMITTING )
                        {
                            printf( "ERROR: concentrator is currently emitting\n" );
                            continue;
                        }
                        else if( tx_status == TX_SCHEDULED )
                        {
                            printf( "WARNING: a downlink was already scheduled, overwritting it...\n" );
                        }
                        else
                        {
                            /* Nothing to do */
                        }
                    }

                    /* send packet to concentrator */
                    pkt.count_us = sx1301ar_cnt2cnt(pkt.count_us, 0, board);
                    pthread_mutex_lock( &mx_concent[board] ); /* may have to wait for a fetch to finish */
                    result = sx1301ar_send( board, &pkt );
                    pthread_mutex_unlock( &mx_concent[board] ); /* free concentrator ASAP */

                    if (result < 0 && sx1301ar_errno == ERR_NO_TX_ENABLE) {
                        printf( "WARNING: TX disabled on board %u, trying with board 0...\n", board);
                        pkt.count_us = sx1301ar_cnt2cnt(pkt.count_us, board, 0);
                        pthread_mutex_lock( &mx_concent[0] ); /* may have to wait for a fetch to finish */
                        result = sx1301ar_send( 0, &pkt );
                        pthread_mutex_unlock( &mx_concent[0] ); /* free concentrator ASAP */
                    }

                    if( result != 0 )
                    {
                        pthread_mutex_lock( &mx_meas_dw );
                        meas_dw.tx_fail += 1;
                        pthread_mutex_unlock( &mx_meas_dw );
                        printf( "WARNING: [jit] sx1301ar_send failed - %s\n", sx1301ar_err_message(sx1301ar_errno) );
                        continue;
                    }
                    else
                    {
                        pthread_mutex_lock( &mx_meas_dw );
                        meas_dw.tx_ok += 1;
                        pthread_mutex_unlock( &mx_meas_dw );
                    }
                }
                else
                {
                    printf( "ERROR: jit_dequeue failed with %d\n", jit_result );
                }
            }
        }
        else if( jit_result == JIT_ERROR_EMPTY )
        {
            /* Do nothing, it can happen */
        }
        else
        {
            printf( "ERROR: jit_peek failed with %d\n", jit_result );
        }
    }
    printf( "\nINFO: End of JiT thread\n" );
    return NULL;
}

/* -------------------------------------------------------------------------- */
/* --- THREAD 5: PARSING GPS MESSAGE AND KEEPING TIME REF IN SYNC ----------- */

static void * thread_gps( const void * arg )
{
    int x; /* return value for sx1301ar_* functions */
    int i;

    /* File descriptor for the GPS TTY (already open and configured) */
    const int fd_gps = *(int *)arg;

    char buff[256]; /* buffer to receive GPS data */
    ssize_t nb_char;

    sx1301ar_nmea_msg_t msg_type;
    int n_sat; /* GPS quality metrics */
    float hdop; /* GPS quality metrics */
    sx1301ar_coord_t coord; /* GPS 3D coordinates */

    bool trig_sync = false;
    struct timespec utc; /* GPS time */
    bool utc_fresh = false; /* true when UTC was updated */
    uint32_t cnt_pps; /* internal timestamp counter captured on PPS edge */
    uint32_t hs_pps; /* high speed counter captured on PPS edge */
    int cnt_pps_delay; /* Number of PPS between two successive GGA NMEA frames */

    /* Initialize some variables before loop */
    memset( buff, 0, sizeof  buff );

    /* Loop until user quits */
    while( (quit_sig != 1) && (exit_sig != 1) )
    {
        /* Wait for NMEA frame (blocking canonical read on serial port) */
        nb_char = read( fd_gps, buff, (sizeof buff) - 1 );
        if( nb_char <= 0 )
        {
            printf( "WARNING: serial port read() returned value <= 0\n" );
            continue;
        }
        else
        {
            buff[nb_char] = '\0'; /* add string terminator */
        }

        /* Determine message type */
        msg_type = NMEA_UNDEFINED;
        x = sx1301ar_get_nmea_type( buff, sizeof buff, &msg_type );
        if( x != 0 )
        {
            printf( "ERROR: sx1301ar_get_nmea_type failed; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
            exit( EXIT_FAILURE );
        }

        /* NMEA parsing tree */
        /* /!\ This code works on uBlox GSP modules, you might have to modify the code if used with another brand of GPS receivers */
        if( msg_type == NMEA_RMC)
        {
            utc_fresh = false;

            /* Parse RMC frame */
            x = sx1301ar_parse_rmc( buff, sizeof buff, &utc, NULL );
            if( x == 0 )
            {
                utc_fresh = true;
                pthread_mutex_lock( &mx_meas_gps );
                meas_gps.utc = utc;
                pthread_mutex_unlock( &mx_meas_gps );
            }
            else if( x < 0 )
            {
                printf( "ERROR: sx1301ar_parse_rmc failed; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
                exit( EXIT_FAILURE );
            }
        }
        else if( msg_type == NMEA_GGA)
        {
            trig_sync = false;

            /* Parse GGA frame */
            x = sx1301ar_parse_gga( buff, sizeof buff, &coord, &n_sat, &hdop );
            if( x == 0 )
            {
                /* If metrics are good enough, trigger a sync */
                /* /!\ ASSUMES GPS SENDS RMC BEFORE GGA, CHECK YOUR GPS MODULE */
                if( (n_sat >= N_SAT_MIN) && (hdop < HDOP_MAX) && (utc_fresh == true) )
                {
                    utc_fresh = false; /* invalidates in case GPS stops sending RMC unexpectedly */
                    trig_sync = true;
                }

                /* store information */
                pthread_mutex_lock( &mx_meas_gps );
                meas_gps.hdop = hdop;
                meas_gps.n_sat = n_sat;
                meas_gps.coord.lat = coord.lat;
                meas_gps.coord.lon = coord.lon;
                meas_gps.coord.alt = coord.alt;
                gps_coord_valid = true;
                pthread_mutex_unlock( &mx_meas_gps );
            }
            else if( x < 0 )
            {
                /* TODO: where is the best place to invalidate obsolete info ? */
                pthread_mutex_lock( &mx_meas_gps );
                gps_coord_valid = false;
                pthread_mutex_unlock( &mx_meas_gps );
                printf( "ERROR: sx1301ar_parse_gga failed; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
                exit( EXIT_FAILURE );
            }
        }

        /* Do a synchronization cycle when all needed info are available */
        if( trig_sync == true )
        {
            trig_sync = false;

            for( i = 0; i < board_nb; i++ ) {

                /* Get timestamp value captured on PPS edge (use mutex to protect hardware access) */
                pthread_mutex_lock( &mx_concent[i] );
                x = sx1301ar_get_trigcnt( i, &cnt_pps );
                x = gwloc_get_hspps( i, &hs_pps );
                pthread_mutex_unlock( &mx_concent[i] );
                if( x != 0 )
                {
                    printf( "ERROR: sx1301ar_get_trigcnt failed; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
                    return NULL;
                }

                /* Attempt sync */
                pthread_mutex_lock( &mx_tref );
                x = sx1301ar_synchronize( &time_ref[i], utc, cnt_pps, hs_pps, &cnt_pps_delay );
                pthread_mutex_lock( &mx_meas_gps );
                meas_gps.cnt_pps_lost += (cnt_pps_delay - 1);
                pthread_mutex_unlock( &mx_meas_gps );
                pthread_mutex_unlock( &mx_tref );
                if( x == 0 )
                {
                    /* Set current xtal err to all boards */
                    x = sx1301ar_set_xtal_err( i, time_ref[i] );
                    if( x != 0 )
                    {
                        printf( "ERROR: failed to set XTAL error to board %u\n", i );
                    }
                }
                else if( x < 0 )
                {
                    printf( "ERROR: sx1301ar_synchronize critically failed; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
                    exit( EXIT_FAILURE );
                }
            }
        }
    }
    printf( "\nINFO: End of GPS thread\n" );
    return NULL;
}

/* -------------------------------------------------------------------------- */
/* --- SUBFUNCTIONS DEFINITION ---------------------------------------------- */

static void sig_handler( int sigio )
{
    if( sigio == SIGQUIT )
    {
        quit_sig = 1;
    }
    else if( (sigio == SIGINT) || (sigio == SIGTERM) )
    {
        exit_sig = 1;
    }
}

static int parse_local_configuration( uint8_t brd , sx1301ar_board_cfg_t *cfg_brd )
{
    int x;

    /* JSON Variables */
    JSON_Value * root_val;
    JSON_Value * val;
    JSON_Array * conf_sx1301_array = NULL;
    JSON_Array * conf_rfchain_array = NULL;
    JSON_Object * root = NULL;
    JSON_Object * brd_conf_obj = NULL;

    /* SPI interface */
    const char *spi_path = NULL;

    if( access( JSON_LOCAL_CONF_DEFAULT, R_OK ) == 0 )
    {
        printf( "INFO: found calibration file %s\n", JSON_LOCAL_CONF_DEFAULT );
        root_val = json_parse_file_with_comments( JSON_LOCAL_CONF_DEFAULT );
        root = json_value_get_object( root_val );
        if( root == NULL )
        {
            printf( "ERROR: %s is not a valid JSON file\n", JSON_LOCAL_CONF_DEFAULT );
            return EXIT_FAILURE;
        }

        conf_sx1301_array = json_object_get_array( root, JSON_SX1301AR_CONF_OBJ );
        if( conf_sx1301_array != NULL )
        {
            brd_conf_obj = json_array_get_object( conf_sx1301_array, brd );
            if( brd_conf_obj == NULL )
            {
                printf( "ERROR: %s array does not include item no%u\n", JSON_SX1301AR_CONF_OBJ, brd );
                return EXIT_FAILURE;
            }
        }
        else
        {
            printf( "ERROR: configuration file does not contain a JSON object named %s\n", JSON_SX1301AR_CONF_OBJ );
            return EXIT_FAILURE;
        }

        /* Check if spidev path is defined in JSON */
        val = json_object_get_value( brd_conf_obj, "spidev" );
        if( json_value_get_type( val ) == JSONString )
        {
            spi_path = json_value_get_string( val );
            /* Open SPI link */
            x = spi_linuxdev_open( spi_path, -1, &spi_context[brd] );
            if( x != 0 )
            {
                printf( "ERROR: opening SPI failed, returned %i on open(%s)\n", x, spi_path );
                return EXIT_FAILURE;
            }
        }

        /* RF chains configuration */
        conf_rfchain_array = json_object_get_array( brd_conf_obj, "rf_chain_conf" );
        x = parse_rfchain_configuration( conf_rfchain_array, cfg_brd );
    }

    return EXIT_SUCCESS;
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

static int parse_sx1301ar_configuration( JSON_Object * brd_conf_obj, uint8_t brd )
{
    int i, j, k;
    int x; /* return code for SX1301ar driver functions */
    char param_name[64]; /* used to generate variable parameter names */
    JSON_Object * conf_chip_obj;
    JSON_Object * conf_chan_obj;
    JSON_Object * conf_lbt_obj;
    JSON_Object * conf_lbtchan_obj;
    JSON_Array * conf_sx1301_array;
    JSON_Array * conf_rfchain_array;
    JSON_Array * conf_lbtchan_array;
    JSON_Value * val;
    uint8_t chan_nb;
    uint32_t tmp_u32;
    int sf_a, sf_b;
    unsigned long long ull = 0, ull2 = 0;
    const char * str;
    int str_len;

    /* SPI interface */
    const char *spi_path = NULL;

    /* Configuration structures */
    sx1301ar_board_cfg_t cfg_brd = sx1301ar_init_board_cfg( );
    sx1301ar_chip_cfg_t cfg_chp;
    sx1301ar_chan_cfg_t cfg_chn;
    sx1301ar_lbt_cfg_t cfg_lbt = sx1301ar_init_lbt_cfg( );

    /* Mandatory: Type of the board */
    val = json_object_get_value( brd_conf_obj, "board_type" );
    if( json_value_get_type( val ) != JSONString )
    {
        printf( "ERROR: impossible to parse mandatory parameter 'board_type'\n" );
        return -1;
    }
    str = json_value_get_string( val );
    if( strncmp( str, "MASTER", strlen(str) ) == 0 )
    {
        printf( "INFO: Master board\n" );
        cfg_brd.board_type = BRD_MASTER;
    }
    else if( strncmp( str, "SLAVE", strlen(str) ) == 0 )
    {
        printf( "INFO: Slave board\n" );
        cfg_brd.board_type = BRD_SLAVE;
    }
    else
    {
        printf( "ERROR: board type %s is not valid\n", str );
        return -1;
    }

    /* Mandatory: Rx frequency of the board (ie. RX freq of the AD9361) */
    val = json_object_get_value( brd_conf_obj, "board_rx_freq" );
    if( json_value_get_type( val ) != JSONNumber )
    {
        printf( "ERROR: impossible to parse mandatory parameter 'board_rx_freq'\n" );
        return -1;
    }
    cfg_brd.rx_freq_hz = (uint32_t)json_value_get_number( val );
    printf( "INFO: board center frequency configured at %u Hz\n", cfg_brd.rx_freq_hz );
    /* Mandatory: Rx bandwidth of the board (ie. RX bw of the AD9361) */
    val = json_object_get_value( brd_conf_obj, "board_rx_bw" );
    if( json_value_get_type( val ) != JSONNumber )
    {
        printf( "ERROR: impossible to parse mandatory parameter 'board_rx_bw'\n" );
        return -1;
    }
    cfg_brd.rx_bw_hz = (uint32_t)json_value_get_number( val );
    printf( "INFO: board RX bandwidth configured at %u Hz\n", cfg_brd.rx_bw_hz );
    /* Optional: radio mode */
    cfg_brd.full_duplex = (bool)json_object_get_boolean( brd_conf_obj, "full_duplex" );

    /* RF chains configuration */
    conf_rfchain_array = json_object_get_array( brd_conf_obj, "rf_chain_conf" );
    if( conf_rfchain_array != NULL )
    {
        x = parse_rfchain_configuration( conf_rfchain_array, &cfg_brd );
    }
    else
    {
        printf( "WARNING: no rf_chain configured\n" );
    }

    /* Listen-Before-Talk configuration */
    conf_lbt_obj = json_object_get_object( brd_conf_obj, "lbt_conf" );
    if( conf_lbt_obj == NULL )
    {
        printf( "INFO: no configuration for LBT\n" );
    }
    else
    {
        val = json_object_get_value( conf_lbt_obj, "enable" );
        if( json_value_get_type(val) == JSONBoolean )
        {
            cfg_lbt.enable = (bool)json_value_get_boolean( val );
        }
        else
        {
            printf( "WARNING: Data type for lbt_conf.enable seems wrong, please check\n" );
            cfg_lbt.enable = false;
        }
        if( cfg_lbt.enable == true )
        {
            val = json_object_get_value( conf_lbt_obj, "rssi_target" );
            if( json_value_get_type( val ) == JSONNumber )
            {
                cfg_lbt.rssi_target = (int8_t)json_value_get_number( val );
            }
            else
            {
                printf( "WARNING: Data type for lbt_conf.rssi_target seems wrong, please check\n" );
                cfg_lbt.rssi_target = 0;
            }
            val = json_object_get_value( conf_lbt_obj, "rssi_shift" );
            if( json_value_get_type( val ) == JSONNumber )
            {
                cfg_lbt.rssi_shift = (int8_t)json_value_get_number( val );
            }
            else
            {
                printf( "WARNING: Data type for lbt_conf.rssi_shift seems wrong, please check\n" );
                cfg_lbt.rssi_shift = 0;
            }
            /* set LBT channels configuration */
            conf_lbtchan_array = json_object_get_array( conf_lbt_obj, "chan_cfg" );
            if( conf_lbtchan_array != NULL )
            {
                cfg_lbt.nb_channel = json_array_get_count( conf_lbtchan_array );
                printf( "INFO: %u LBT channels configured\n", cfg_lbt.nb_channel );
            }
            for( i = 0; i < (int)cfg_lbt.nb_channel; i++ )
            {
                /* Sanity check */
                if( i >= SX1301AR_LBT_CHANNEL_NB_MAX )
                {
                    printf( "ERROR: LBT channel %d not supported, skip it\n", i  );
                    break;
                }
                /* Get LBT channel configuration object from array */
                conf_lbtchan_obj = json_array_get_object( conf_lbtchan_array, i );

                /* Channel frequency */
                val = json_object_dotget_value( conf_lbtchan_obj, "freq_hz" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_lbt.channels[i].freq_hz = (uint32_t)json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for lbt_cfg.channels[%d].freq_hz seems wrong, please check\n", i );
                    cfg_lbt.channels[i].freq_hz = 0;
                }

                /* Channel scan time */
                val = json_object_dotget_value( conf_lbtchan_obj, "scan_time_us" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_lbt.channels[i].scan_time_us = (uint16_t)json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for lbt_cfg.channels[%d].scan_time_us seems wrong, please check\n", i );
                    cfg_lbt.channels[i].scan_time_us = 0;
                }
            }

            /* all parameters parsed, submitting configuration to the HAL */
            if( sx1301ar_conf_lbt( brd, &cfg_lbt ) != 0 )
            {
                printf( "ERROR: Failed to configure LBT\n" );
                return -1;
            }
        }
        else
        {
            printf( "INFO: LBT is disabled\n" );
        }
    }

    /* Optional: FSK sync word */
    val = json_object_get_value( brd_conf_obj, "FSK_sync" );
    if( json_value_get_type( val ) == JSONString )
    {
        str = json_value_get_string( val );
        str_len = strlen( str );
        if( (str_len < 2) || (str_len > 16) || (str_len%2 != 0) )
        {
            printf( "ERROR: impossible to parse parameter 'FSK_sync'\n" );
            return -1;
        }
        i = sscanf( str, "%llx", &ull );
        if( i != 1 )
        {
            printf( "ERROR: failed to parse hex string for 'FSK_sync'\n" );
            return -1;
        }
        cfg_brd.fsk_sync_size = str_len / 2;
        cfg_brd.fsk_sync_word = (uint64_t)ull;
    }

    /* Optional: enable cohabitation setting for LoRa MAC public networks */
    cfg_brd.loramac_public = (bool)json_object_get_boolean( brd_conf_obj, "loramac_public" );

    /* Getting AES-128 decryption key */
    val = json_object_get_value( brd_conf_obj, "aes_key" );
    if( json_value_get_type( val ) == JSONString )
    {
        str = json_value_get_string( val );
        str_len = strlen( str );
        if( str_len != 32 ) /* 16 bytes => 32 characters */
        {
            printf( "ERROR: wrong size for AES-128 key\n" );
        }
        else
        {
            i = sscanf( json_value_get_string( val ), "%16llx%16llx", &ull, &ull2 );
            if( i != 2 )
            {
                printf( "ERROR: failed to parse hex string for AES-128 key\n" );
            }
            else
            {
                for( i = 0; i < 8; i++ )
                {
                    cfg_brd.aes_key[i]   = (uint8_t)((ull  >> (56 - i*8)) & 0xFF);
                    cfg_brd.aes_key[i+8] = (uint8_t)((ull2 >> (56 - i*8)) & 0xFF);
                }
                printf( "INFO: gateway AES-128 key is configured to %016llX%016llX\n", ull, ull2 );
            }
        }
    }
    else
    {
        printf( "ERROR: wrong format for AES-128 key\n" );
    }

    /* Get room temperature used as Tref reference */
    cfg_brd.room_temp_ref = (int8_t)json_object_get_number( brd_conf_obj, "calibration_temperature_celsius_room" );
    printf( "INFO: Calibration room temperature (Tref) set to %u oC\n", cfg_brd.room_temp_ref );

    /* Get radio temperature sensor reference value (for Tref°C) */
    cfg_brd.ad9361_temp_ref = (uint8_t)json_object_get_number( brd_conf_obj, "calibration_temperature_code_ad9361" );
    printf( "INFO: Calibration radio temperature code at [Tref] set to %u\n", cfg_brd.ad9361_temp_ref );

    /* Get DSP status report interval */
    cfg_brd.dsp_stat_interval = (uint8_t)json_object_get_number( brd_conf_obj, "dsp_stat_interval" );

    /* Get the number of dsp on the board */
    val = json_object_get_value( brd_conf_obj, "nb_dsp" );
    if( json_value_get_type( val ) != JSONNumber )
    {
        printf( "ERROR: impossible to parse mandatory parameter 'nb_dsp'\n" );
        return -1;
    }
    cfg_brd.nb_dsp = (uint32_t)json_value_get_number( val );
    printf( "INFO: %u dsp on the board\n", cfg_brd.nb_dsp );

    /* Get the number of chip on the board */
    conf_sx1301_array = json_object_get_array( brd_conf_obj, "SX1301_conf" );
    if( conf_sx1301_array != NULL )
    {
        cfg_brd.nb_chip = (uint8_t)json_array_get_count( conf_sx1301_array );
    }

    /* Local Overload config file */
    parse_local_configuration( brd, &cfg_brd );

    /* Check if spidev path is defined in JSON */
    val = json_object_get_value( brd_conf_obj, "spidev" );
    if( json_value_get_type( val ) == JSONString )
    {
        spi_path = json_value_get_string( val );
        /* Open SPI link */
        x = spi_linuxdev_open( spi_path, -1, &spi_context[brd] );
        if( x != 0 )
        {
            printf( "ERROR: opening SPI failed, returned %i on open(%s)\n", x, spi_path );
            return EXIT_FAILURE;
        }
    }

    /* Configure SPI devices to master/slave modes (Needed for ATMEL CPU only)
     SPI 0: HOST <-> FPGA
     SPI 1: HOST/DSP <-> Flash
     */
    x = spi_set_mode( 0, SPI_MODE_MASTER );
    if( x != 0 )
    {
        printf( "ERROR: setting SPI mode failed, returned %i\n", x );
        return EXIT_FAILURE;
    }
    x = spi_set_mode( 1, SPI_MODE_SLAVE ); /* So that DSP can access the flash for booting */
    if( x != 0 )
    {
        printf( "ERROR: setting SPI mode failed, returned %i\n", x );
        return EXIT_FAILURE;
    }

    /* Associate with SPI */
    switch( brd )
    {
    case 0:
        cfg_brd.spi_read = &spi1_read;
        cfg_brd.spi_write = &spi1_write;
        break;
#if ( SX1301AR_MAX_BOARD_NB > 1 )
    case 1:
        cfg_brd.spi_read = &spi2_read;
        cfg_brd.spi_write = &spi2_write;
        break;
#endif
#if ( SX1301AR_MAX_BOARD_NB > 2 )
    case 2:
        cfg_brd.spi_read = &spi3_read;
        cfg_brd.spi_write = &spi3_write;
        break;
#endif
#if ( SX1301AR_MAX_BOARD_NB > 3 )
    case 3:
        cfg_brd.spi_read = &spi4_read;
        cfg_brd.spi_write = &spi4_write;
        break;
#endif
    default:
        printf( "ERROR: Board %u not supported\n", brd );
        return EXIT_FAILURE;
        break;
    }

    /* Commit board configuration */
    x = sx1301ar_conf_board( brd, &cfg_brd );
    if( x != 0 )
    {
        printf( "ERROR: sx1301ar_conf_board failed; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
        return -1;
    }

    /* SX1301 chip configuration */
    if( conf_sx1301_array != NULL )
    {
        /* Chips configuration loop */
        for( i = 0; i < (int)json_array_get_count( conf_sx1301_array ); ++i )
        {
            /* Sanity check */
            if( i >= SX1301AR_BOARD_CHIPS_NB)
            {
                printf( "ERROR: SX1301 chip %d not supported, skip it\n", i );
                break;
            }

            /* (re)initialize chip configuration structure */
            cfg_chp = sx1301ar_init_chip_cfg( );

            /* Point to chip configuration */
            conf_chip_obj = json_array_get_object( conf_sx1301_array, i );

            cfg_chp.enable = (bool)json_object_get_boolean( conf_chip_obj, "chip_enable" );
            if( cfg_chp.enable == false )
            {
                printf( "INFO: chip %i disabled\n", i );
                x = sx1301ar_conf_chip( brd, i, &cfg_chp );
                if( x != 0 )
                {
                    printf( "ERROR: sx1301ar_conf_chip failed for chip %i; %s\n", i, sx1301ar_err_message( sx1301ar_errno ) );
                    return -1;
                }
                /* Skip to next chip */
                continue;
            }

            /* Configure enbaled chip */
            printf( "INFO: chip %i enabled\n", i );
            /* Mandatory: center frequency of the chip (ie. RX freq of each SX1301) */
            val = json_object_get_value( conf_chip_obj, "chip_center_freq" );
            if( json_value_get_type( val ) != JSONNumber )
            {
                printf( "ERROR: failed to parse mandatory parameter chip_center_freq for chip %i\n", i );
                return -1;
            }
            cfg_chp.freq_hz = (uint32_t)json_value_get_number( val );

            // Rx RF chain select
            val = json_object_get_value( conf_chip_obj, "chip_rf_chain" );
            if( json_value_get_type( val ) != JSONNumber )
            {
                printf( "ERROR: failed to parse mandatory parameter chip_rf_chain for chip %i\n", i );
                return -1;
            }
            cfg_chp.rf_chain = (uint8_t)json_value_get_number( val );

            /* Commit chip configuration */
            printf( "INFO: chip %i center frequency configured at %u Hz\n", i, cfg_chp.freq_hz );
            x = sx1301ar_conf_chip( brd, i, &cfg_chp );
            if( x != 0 )
            {
                printf( "ERROR: sx1301ar_conf_chip failed for chip %i; %s\n", i, sx1301ar_err_message( sx1301ar_errno ) );
                return -1;
            }

            /* Channels configuration loop */
            for( j = 0; j < SX1301AR_CHIP_CHAN_NB; ++j )
            {
                chan_nb = (i << 4) + j; /* generate board-level channel number */
                cfg_chn = sx1301ar_init_chan_cfg( ); /* (re)initialize channel configuration structure */

                /* Point to channel configuration */
                if( j < SX1301AR_CHIP_MULTI_NB )
                {
                    sprintf( param_name, "chan_multiSF_%i", j );
                    conf_chan_obj = json_object_get_object( conf_chip_obj, param_name );
                }
                else if( j == SX1301AR_CHIP_LSA_IDX )
                {
                    conf_chan_obj = json_object_get_object( conf_chip_obj, "chan_LoRa_std" );
                }
                else if( j == SX1301AR_CHIP_FSK_IDX )
                {
                    conf_chan_obj = json_object_get_object( conf_chip_obj, "chan_FSK" );
                }

                /* Channel disabled if configuration is absent */
                if( conf_chan_obj == NULL )
                {
                    cfg_chn.enable = false; /* should already be default value */
                    printf( "INFO: channel 0x%02X disabled\n", chan_nb );
                    x = sx1301ar_conf_chan( brd, chan_nb, &cfg_chn );
                    if( x != 0 )
                    {
                        printf( "ERROR: sx1301ar_conf_chan failed for channel 0x%02X; %s\n", chan_nb, sx1301ar_err_message( sx1301ar_errno ) );
                        return -1;
                    }
                    continue;
                }
                else
                {
                    cfg_chn.enable = true;
                }

                /* Mandatory: center frequency of the channel */
                val = json_object_get_value( conf_chan_obj, "chan_rx_freq" );
                if( json_value_get_type( val ) != JSONNumber )
                {
                    printf( "ERROR: failed to parse mandatory parameter chan_rx_freq for chip 0x%02X\n", chan_nb );
                    return -1;
                }
                cfg_chn.freq_hz = (uint32_t)json_value_get_number( val );

                /* Parse parameters specific to each type of channel */
                if( j < SX1301AR_CHIP_MULTI_NB )
                {
                    cfg_chn.bandwidth = BW_125K; /* not really necessary */

                    /* LoRa spreading factor or spreading factor range (multi-SF modem) */
                    val = json_object_get_value( conf_chan_obj, "spread_factor" );
                    if( json_value_get_type( val ) == JSONString )
                    {
                        k = sscanf( json_value_get_string( val ), "%i-%i", &sf_a, &sf_b );
                        if( k == 2 )
                        {
                            cfg_chn.modrate = sx1301ar_sf_range_nb2enum( sf_a, sf_b );
                        }
                        else if( k == 1 )
                        {
                            cfg_chn.modrate = sx1301ar_sf_nb2enum( sf_a );
                        }
                        else
                        {
                            cfg_chn.modrate = MR_UNDEFINED;
                        }
                    }
                    else if( json_value_get_type( val ) == JSONNumber )
                    {
                        cfg_chn.modrate = sx1301ar_sf_nb2enum( (int)json_value_get_number( val ) );
                    }
                    else
                    {
                        cfg_chn.modrate = MR_UNDEFINED;
                    }
                }
                else if( j == SX1301AR_CHIP_LSA_IDX )
                {
                    /* Bandwidth */
                    tmp_u32 = (uint32_t)json_object_get_number( conf_chan_obj, "bandwidth" );
                    cfg_chn.bandwidth = sx1301ar_bw_nb2enum( tmp_u32 );

                    /* SF */
                    tmp_u32 = (uint32_t)json_object_get_number( conf_chan_obj, "spread_factor" );
                    cfg_chn.modrate = sx1301ar_sf_nb2enum( tmp_u32 );
                }
                else if( j == SX1301AR_CHIP_FSK_IDX )
                {
                    /* Bandwidth */
                    tmp_u32 = (uint32_t)json_object_get_number( conf_chan_obj, "bandwidth" );
                    cfg_chn.bandwidth = sx1301ar_bw_nb2enum( tmp_u32 );

                    /* Bit rate */
                    tmp_u32 = (uint32_t)json_object_get_number( conf_chan_obj, "bit_rate" );
                    cfg_chn.modrate = tmp_u32;
                }

                /* Commit channel configuration */
                if( j < SX1301AR_CHIP_MULTI_NB )
                {
                    printf( "INFO: LoRa multi-SF channel 0x%02x configured at %u Hz, BW 125kHz, SF %i to %i\n", chan_nb, cfg_chn.freq_hz, sx1301ar_sf_min_enum2nb( cfg_chn.modrate ), sx1301ar_sf_max_enum2nb( cfg_chn.modrate ) );
                }
                else if( j == SX1301AR_CHIP_LSA_IDX )
                {
                    printf( "INFO: LoRa stand-alone channel 0x%02x configured at %u Hz, BW %li Hz, SF %i\n", chan_nb, cfg_chn.freq_hz, sx1301ar_bw_enum2nb( cfg_chn.bandwidth ), sx1301ar_sf_enum2nb( cfg_chn.modrate ) );
                }
                else if( j == SX1301AR_CHIP_FSK_IDX )
                {
                    printf( "INFO: FSK channel 0x%02x configured at %u Hz, BW %li Hz, bit rate %li\n", chan_nb, cfg_chn.freq_hz, sx1301ar_bw_enum2nb( cfg_chn.bandwidth ), (long)cfg_chn.modrate );
                }
                x = sx1301ar_conf_chan( brd, chan_nb, &cfg_chn );
                if( x != 0 )
                {
                    printf( "ERROR: sx1301ar_conf_chan failed for channel 0x%02X; %s\n", chan_nb, sx1301ar_err_message( sx1301ar_errno ) );
                    return -1;
                }
            }
        }
    }

    return 0;
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */
static int parse_rfchain_configuration( JSON_Array * conf_rfchain_array, sx1301ar_board_cfg_t *cfg_brd )
{
    int i, j;
    JSON_Object * conf_txgain_obj;
    JSON_Object * conf_rfchain_obj;
    JSON_Array * conf_txlut_array;
    JSON_Value * val;

    for( i = 0; i < (int) json_array_get_count( conf_rfchain_array ); i++ )
    {
        /* Sanity check */
        if( i >= SX1301AR_BOARD_RFCHAIN_NB )
        {
            printf( "ERROR: rf_chain %d not supported, skip it\n", i );
            break;
        }

        /* Get RF chain conf object */
        conf_rfchain_obj = json_array_get_object( conf_rfchain_array, i );

        /* Parse configuration */
        cfg_brd->rf_chain[i].rx_enable = (bool) json_object_get_boolean( conf_rfchain_obj, "rx_enable" );
        cfg_brd->rf_chain[i].tx_enable = (bool) json_object_get_boolean( conf_rfchain_obj, "tx_enable" );
        cfg_brd->rf_chain[i].rssi_offset = (float) json_object_get_number( conf_rfchain_obj, "rssi_offset" );

        val = json_object_get_value( conf_rfchain_obj, "rssi_offset_coeff_a" );
        if( json_value_get_type( val ) == JSONNumber ) cfg_brd->rf_chain[i].rssi_offset_coeff_a = (int16_t) json_value_get_number( val );
        val = json_object_get_value( conf_rfchain_obj, "rssi_offset_coeff_b" );
        if( json_value_get_type( val ) == JSONNumber ) cfg_brd->rf_chain[i].rssi_offset_coeff_b = (int16_t) json_value_get_number( val );

        printf( "INFO: rf_chain %d: RSSI offset configured at %.1f, a=%d, b=%d\n", i, cfg_brd->rf_chain[i].rssi_offset,
                cfg_brd->rf_chain[i].rssi_offset_coeff_a, cfg_brd->rf_chain[i].rssi_offset_coeff_b );

        /* Record TX limits if given */
        tx_freq_min[i] = (uint32_t)json_object_get_number( conf_rfchain_obj, "tx_freq_min" );
        tx_freq_max[i] = (uint32_t)json_object_get_number( conf_rfchain_obj, "tx_freq_max" );

        /* set configuration for tx gains */
        conf_txlut_array = json_object_get_array( conf_rfchain_obj, "tx_lut" );
        if( conf_txlut_array != NULL )
        {
            cfg_brd->rf_chain[i].tx_lut.size = json_array_get_count( conf_txlut_array );
            printf( "INFO: rf_chain %d: Configuring Tx LUT with %u indexes\n", i, cfg_brd->rf_chain[i].tx_lut.size );
            for( j = 0; j < (int) cfg_brd->rf_chain[i].tx_lut.size; j++ )
            {
                /* Sanity check */
                if( j >= SX1301AR_BOARD_MAX_LUT_NB )
                {
                    printf( "ERROR: TX LUT index %d not supported, skip it\n", j );
                    break;
                }

                /* Get TX gain object from LUT */
                conf_txgain_obj = json_array_get_object( conf_txlut_array, j );

                /* rf power */
                val = json_object_dotget_value( conf_txgain_obj, "rf_power" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].rf_power = (int8_t) json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for %s[%d] seems wrong, please check\n", "rf_power", j );
                    cfg_brd->rf_chain[i].tx_lut.lut[j].rf_power = 0;
                }

                /* FPGA dig gain */
                val = json_object_dotget_value( conf_txgain_obj, "fpga_dig_gain" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].fpga_dig_gain = (uint8_t) json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for %s[%d] seems wrong, please check\n", "fpga_dig_gain", j );
                    cfg_brd->rf_chain[i].tx_lut.lut[j].fpga_dig_gain = 0;
                }

                /* AD9361 atten */
                val = json_object_get_value( conf_txgain_obj, "ad9361_atten" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_gain.atten = (uint16_t) json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for %s[%d] seems wrong, please check\n", "ad9361_atten", j );
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_gain.atten = 0;
                }

                /* AD9361 auxdac vref lsb */
                val = json_object_dotget_value( conf_txgain_obj, "ad9361_auxdac_vref" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_gain.auxdac_vref = (uint8_t) json_value_get_number( val );
                }
                else
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_gain.auxdac_vref = 0;
                }

                /* AD9361 auxdac word */
                val = json_object_dotget_value( conf_txgain_obj, "ad9361_auxdac_word" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_gain.auxdac_word = (uint16_t) json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for %s[%d] seems wrong, please check\n", "ad9361_auxdac_word", j );
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_gain.auxdac_word = 0;
                }

                /* AD9361 temperature compensation coeff A */
                val = json_object_dotget_value( conf_txgain_obj, "ad9361_tcomp_coeff_a" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_tcomp.coeff_a = (int16_t) json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for %s[%d] seems wrong, please check\n", "ad9361_tcomp_coeff_a", j );
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_tcomp.coeff_a = 0;
                }

                /* AD9361 temperature compensation coeff B */
                val = json_object_dotget_value( conf_txgain_obj, "ad9361_tcomp_coeff_b" );
                if( json_value_get_type( val ) == JSONNumber )
                {
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_tcomp.coeff_b = (int16_t) json_value_get_number( val );
                }
                else
                {
                    printf( "WARNING: Data type for %s[%d] seems wrong, please check\n", "ad9361_tcomp_coeff_b", j );
                    cfg_brd->rf_chain[i].tx_lut.lut[j].ad9361_tcomp.coeff_b = 0;
                }
            }
        }
    }

    return 0;
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

static int parse_gateway_configuration( JSON_Object * gw_conf_obj, gw_conf_t * gwc )
{
    JSON_Value * val;
    unsigned long long ull = 0;
    uint32_t u32 = 0;

    /* Getting network parameters (only those necessary for the packet logger) */
    val = json_object_get_value( gw_conf_obj, "gateway_ID" );
    if( json_value_get_type( val ) == JSONString )
    {
        sscanf( json_value_get_string( val ), "%llx", &ull );
        gwc->gw_id = ull;
    }
    else
    {
        gwc->gw_id = 0;
    }
    printf( "INFO: gateway MAC address is configured to %016llX\n", ull );

    /* Get server host name or IP address */
    val = json_object_get_value( gw_conf_obj, "server_address" );
    if( json_value_get_type( val ) == JSONString )
    {
        strncpy( gwc->serv_addr, json_value_get_string( val ), sizeof gwc->serv_addr );
    }
    printf( "INFO: server hostname or IP address is configured to \"%s\"\n", gwc->serv_addr );

    /* Get up and down ports */
    val = json_object_get_value( gw_conf_obj, "serv_port_up" );
    if( val != NULL )
    {
        snprintf( gwc->serv_port_up, sizeof gwc->serv_port_up, "%u", (uint16_t)json_value_get_number( val ) );
        printf( "INFO: upstream port is configured to \"%s\"\n", gwc->serv_port_up );
    }

    val = json_object_get_value( gw_conf_obj, "serv_port_down" );
    if( val != NULL )
    {
        snprintf( gwc->serv_port_down, sizeof gwc->serv_port_down, "%u", (uint16_t)json_value_get_number( val ) );
        printf( "INFO: downstream port is configured to \"%s\"\n", gwc->serv_port_down );
    }

    /* Get keep-alive interval (in seconds) for downstream (-1 to disable) */
    val = json_object_get_value( gw_conf_obj, "keepalive_interval" );
    if( val != NULL )
    {
        gwc->keepalive_time = (int)json_value_get_number( val );
        printf( "INFO: downstream keep-alive interval is configured to %u seconds\n", gwc->keepalive_time );
    }

    /* Get interval (in seconds) for statistics display */
    val = json_object_get_value( gw_conf_obj, "stat_interval" );
    if( val != NULL )
    {
        gwc->stat_interval = (unsigned)json_value_get_number( val );
        printf( "INFO: statistics display interval is configured to %u seconds\n", gwc->stat_interval );
    }

    /* Get time-out value (in ms) for upstream datagrams */
    val = json_object_get_value( gw_conf_obj, "push_timeout_ms" );
    if( val != NULL )
    {
        (gwc->push_timeout_half).tv_usec = 500 * (long int)json_value_get_number( val );
        printf( "INFO: upstream PUSH_DATA time-out is configured to %u ms\n", (unsigned)((gwc->push_timeout_half).tv_usec / 500) );
    }

    /* Packet filtering parameters */
    val = json_object_get_value( gw_conf_obj, "forward_crc_valid" );
    if( json_value_get_type( val ) == JSONBoolean )
    {
        gwc->fwd_valid_pkt = (bool)json_value_get_boolean( val );
    }
    printf( "INFO: packets received with a valid CRC will%s be forwarded\n", (gwc->fwd_valid_pkt ? "" : " NOT") );

    val = json_object_get_value( gw_conf_obj, "forward_crc_error" );
    if( json_value_get_type( val ) == JSONBoolean )
    {
        gwc->fwd_error_pkt = (bool)json_value_get_boolean( val );
    }
    printf( "INFO: packets received with a CRC error will%s be forwarded\n", (gwc->fwd_error_pkt ? "" : " NOT") );

    val = json_object_get_value( gw_conf_obj, "forward_crc_disabled" );
    if( json_value_get_type( val ) == JSONBoolean )
    {
        gwc->fwd_nocrc_pkt = (bool)json_value_get_boolean( val );
    }
    printf( "INFO: packets received with no CRC will%s be forwarded\n", (gwc->fwd_nocrc_pkt ? "" : " NOT") );

    /* Auto-quit threshold (optional) */
    val = json_object_get_value( gw_conf_obj, "autoquit_threshold" );
    if( val != NULL )
    {
        gwc->autoquit_threshold = (uint32_t)json_value_get_number( val );
        printf( "INFO: Auto-quit after %u non-acknowledged PULL_DATA\n", gwc->autoquit_threshold );
    }

    /* Getting statistics mote address */
    val = json_object_get_value( gw_conf_obj, "link_mote" );
    if( json_value_get_type( val ) == JSONString )
    {
        sscanf( json_value_get_string( val ), "%x", &u32 );
        gwc->link_mote = u32;
        printf( "INFO: Link testing mote address is configured to %08X\n", u32 );
    }
    else
    {
        gwc->link_mote = 0;
        printf( "INFO: Link testing mote address is configured not configured\n" );
    }

    return 0;
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

static int parse_gps_configuration( JSON_Object * gps_conf_obj )
{
    JSON_Value * val;
    int x; /* return code for SX1301ar driver functions */
    sx1301ar_gps_cfg_t cfg_gps = sx1301ar_init_gps_cfg( );

    /* Associate with I2C for the GPS */
    cfg_gps.i2c_read = &i2c_read;
    cfg_gps.i2c_write = &i2c_write;
    cfg_gps.i2c_dev_addr = I2CADDR_GPS;

    if( gps_conf_obj != NULL )
    {
        val = json_object_get_value( gps_conf_obj, "fixed_altitude" );
        if( json_value_get_type( val ) == JSONBoolean )
        {
            cfg_gps.bfix_altitude = (bool)json_value_get_boolean( val );
        }

        val = json_object_get_value( gps_conf_obj, "gw_altitude" );
        if( val != NULL )
        {
            cfg_gps.fix_altitude = (uint16_t)json_value_get_number( val );
            if( cfg_gps.bfix_altitude == true )
            {
                printf( "INFO: altitude is configured to %u m\n", cfg_gps.fix_altitude );
            }
            else
            {
                printf( "INFO: altitude is not configured\n" );
            }
        }
    }

    /* Commit GPS configuration parameters */
    x = sx1301ar_conf_gps( &cfg_gps );
    if( x != 0 )
    {
        printf( "ERROR: sx1301ar_conf_gps failed; %s\n", sx1301ar_err_message( sx1301ar_errno ) );
        return -1;
    }

    return 0;
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

static void usage( void )
{
    printf( "~~~ Library version string~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" );
    printf( " %s\n", sx1301ar_version_info( SX1301AR_BOARD_MASTER, NULL, NULL ) );
    printf( "~~~ Available options ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" );
    printf( " -h  print this help\n" );
    printf( " -d <path>  use Linux SPI device driver\n" );
    printf( "            => default path: " LINUXDEV_PATH_DEFAULT "\n" );
    printf( " -g <optional path>  use GPS receiver for synchronization\n" );
    printf( "            => default path: " GPSTTY_PATH_DEFAULT "\n" );
    printf( "            warning: NO SPACE between -g and gps path!!\n" );
    printf( " -c <filename>  use config file other than 'config.json'\n" );
    printf( "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" );
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

static int send_tx_ack( const gw_conf_t *gwc, uint8_t token_h, uint8_t token_l, jit_error_t error )
{
    uint8_t buff_ack[64]; /* buffer to give feedback to server */
    int j, buff_index;

    /* reset buffer */
    memset( &buff_ack, 0, sizeof buff_ack );

    /* Prepare downlink feedback to be sent to server */
    buff_ack[0] = PROTOCOL_VERSION;
    buff_ack[1] = token_h;
    buff_ack[2] = token_l;
    buff_ack[3] = PKT_TX_ACK;
    *(uint32_t *)(buff_ack + 4) = gwc->net_mac_h;
    *(uint32_t *)(buff_ack + 8) = gwc->net_mac_l;
    buff_index = 12; /* 12-byte header */

    /* Put no JSON string if there is nothing to report */
    if( error != JIT_ERROR_OK )
    {
        /* start of JSON structure */
        j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "{\"txpk_ack\":{" );
        if( j > 0 )
        {
            buff_index += j;
        }
        else
        {
            printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
            return -1;
        }
        /* set downlink error status in JSON structure */
        j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"error\":" );
        if( j > 0 )
        {
            buff_index += j;
        }
        else
        {
            printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
            return -1;
        }
        switch( error )
        {
            case JIT_ERROR_FULL:
            case JIT_ERROR_COLLISION_PACKET:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"COLLISION_PACKET\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                /* update stats */
                pthread_mutex_lock( &mx_meas_dw );
                meas_dw.tx_rejected_collision_packet += 1;
                pthread_mutex_unlock( &mx_meas_dw );
                break;
            case JIT_ERROR_TOO_LATE:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"TOO_LATE\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                /* update stats */
                pthread_mutex_lock( &mx_meas_dw );
                meas_dw.tx_rejected_too_late += 1;
                pthread_mutex_unlock( &mx_meas_dw );
                break;
            case JIT_ERROR_TOO_EARLY:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"TOO_EARLY\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                /* update stats */
                pthread_mutex_lock( &mx_meas_dw );
                meas_dw.tx_rejected_too_early += 1;
                pthread_mutex_unlock( &mx_meas_dw );
                break;
            case JIT_ERROR_COLLISION_BEACON:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"COLLISION_BEACON\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                /* update stats */
                pthread_mutex_lock( &mx_meas_dw );
                meas_dw.tx_rejected_collision_beacon += 1;
                pthread_mutex_unlock( &mx_meas_dw );
                break;
            case JIT_ERROR_TX_FREQ:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"TX_FREQ\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                break;
            case JIT_ERROR_TX_POWER:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"TX_POWER\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                break;
            case JIT_ERROR_GPS_UNLOCKED:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"GPS_UNLOCKED\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                break;
            default:
                j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "\"UNKNOWN\"" );
                if( j > 0 )
                {
                    buff_index += j;
                }
                else
                {
                    printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
                    return -1;
                }
                break;
        }
        /* end of JSON structure */
        j = snprintf( (char *)(buff_ack + buff_index), 64-buff_index, "}}" );
        if( j > 0 )
        {
            buff_index += j;
        }
        else
        {
            printf( "ERROR: [up] snprintf failed line %u\n", (__LINE__ - 7) );
            return -1;
        }
    }

    buff_ack[buff_index] = 0; /* add string terminator, for safety */

    if( error != JIT_ERROR_OK )
    {
        printf( "\nJSON tx_ack (%d): %s\n", buff_index, (char *)(buff_ack + 12) ); /* DEBUG: display JSON payload */
    }

    /* send datagram to server */
    return sendto( sock_down, (void *)buff_ack, buff_index, 0, (struct sockaddr *)&sa_down, salen_down);
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

static int spi1_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_read( header, spi_context[0], address, data, size, status );
}

static int spi1_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_write( header, spi_context[0], address, data, size, status );
}

#if ( SX1301AR_MAX_BOARD_NB > 1 )
static int spi2_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_read( header, spi_context[1], address, data, size, status );
}

static int spi2_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_write( header, spi_context[1], address, data, size, status );
}
#endif

#if ( SX1301AR_MAX_BOARD_NB > 2 )
static int spi3_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_read( header, spi_context[2], address, data, size, status );
}

static int spi3_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_write( header, spi_context[2], address, data, size, status );
}
#endif

#if ( SX1301AR_MAX_BOARD_NB > 3 )
static int spi4_read( uint8_t header, uint16_t address, uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_read( header, spi_context[3], address, data, size, status );
}

static int spi4_write( uint8_t header, uint16_t address, const uint8_t * data, uint32_t size, uint8_t * status )
{
    return spi_linuxdev_write( header, spi_context[3], address, data, size, status );
}
#endif

static int i2c_read( uint8_t device_addr, uint8_t reg_addr, uint8_t *data, uint16_t nb_bytes)
{
    return i2c_linuxdev_read_reg( i2c_dev, device_addr, reg_addr, data, nb_bytes );
}

static int i2c_write( uint8_t device_addr, uint8_t *data, uint16_t nb_bytes )
{
    return i2c_linuxdev_write_burst( i2c_dev, device_addr, data, nb_bytes );
}

/* ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ */

#define DEBUG_UPLINK_HISTORY 0

static void _history_print( void )
{
#if DEBUG_UPLINK_HISTORY
    int i;

    /* internal function, no mutex protection */

    for(i = 0; i < UPLINK_HISTORY_MAX; i++ )
    {
        printf(" %d: [%02X%02X] - %ld\n", i, up_history.uplinks[i].token_h, up_history.uplinks[i].token_l, up_history.uplinks[i].time );
    }
#endif
}

static void _history_remove( int idx )
{
    /* internal function, no mutex protection */

    /* remove from the history, replace with last index */
    if( up_history.size > 1 )
    {
        up_history.uplinks[idx].token_l = up_history.uplinks[up_history.size-1].token_l;
        up_history.uplinks[idx].token_h = up_history.uplinks[up_history.size-1].token_h;
        up_history.uplinks[idx].time = up_history.uplinks[up_history.size-1].time;
        up_history.uplinks[up_history.size-1].token_l = 0;
        up_history.uplinks[up_history.size-1].token_h = 0;
        up_history.uplinks[up_history.size-1].time = 0;
        up_history.size -= 1;
    }
    else
    {
        up_history.uplinks[idx].token_l = 0;
        up_history.uplinks[idx].token_h = 0;
        up_history.uplinks[idx].time = 0;
        up_history.size = 0;
    }
}

static void history_init( void )
{
    pthread_mutex_lock( &mx_up_hist );

    memset( &up_history, 0, sizeof up_history );

    pthread_mutex_unlock( &mx_up_hist );
}

static int history_save( uint8_t token_l, uint8_t token_h )
{
    pthread_mutex_lock( &mx_up_hist );

    if( up_history.size == UPLINK_HISTORY_MAX )
    {
        printf( "ERROR: uplink history is full\n" );
        pthread_mutex_unlock( &mx_up_hist );
        return -1;
    }

    up_history.uplinks[up_history.size].token_l = token_l;
    up_history.uplinks[up_history.size].token_h = token_h;
    up_history.uplinks[up_history.size].time = time( NULL );

    //printf( "HISTORY: added [%02X%02X] at index %u (time=%ld)\n", token_h, token_l, up_history.size, up_history.uplinks[up_history.size].time );

    up_history.size += 1;

    _history_print( );

    pthread_mutex_unlock( &mx_up_hist );
    return 0;
}

static int history_check( uint8_t token_l, uint8_t token_h )
{
    int i;

    pthread_mutex_lock( &mx_up_hist );

    for( i = 0; i < up_history.size; i++ )
    {
        if( (up_history.uplinks[i].token_l == token_l) && (up_history.uplinks[i].token_h == token_h) )
        {
            /* Found token */
            break;
        }
    }

    if( i == up_history.size )
    {
        /* token not found */
        pthread_mutex_unlock( &mx_up_hist );
        return -1;
    }
    else
    {
        //printf( "HISTORY: found [%02X%02X] at index %u\n", token_h, token_l, i );
        _history_remove( i ); /* remove from the history, replace with last one */
    }

    _history_print( );

    pthread_mutex_unlock( &mx_up_hist );

    return 0;
}

static void history_purge( void )
{
    int i;
    time_t now;

    pthread_mutex_lock( &mx_up_hist );

    i = 0;
    while( i < UPLINK_HISTORY_MAX )
    {
        now = time( NULL );
        if( (up_history.uplinks[i].time != 0) && ((now - up_history.uplinks[i].time) > 10) )
        {
            printf( "HISTORY: purge [%02X%02X] at index %d (time=%ld - now=%ld)\n", up_history.uplinks[i].token_h, up_history.uplinks[i].token_l, i, up_history.uplinks[i].time, now );
            _history_remove( i ); /* remove from the history, replace with last one */

            /* don't increment loop index as we want to test the new replacing index */
        }
        else
        {
            i += 1;
        }
    }

    _history_print( );

    pthread_mutex_unlock( &mx_up_hist );
}

/* --- EOF ------------------------------------------------------------------ */
