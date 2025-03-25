/***********************************************************************
 *
 * NAME
 *   HttpGet - version 1.3
 *
 * DESCRIPTION
 *   Get a file using HTTP or GOPHER.
 *
 * COMPILATION
 *   Compile with: gcc httpget.c -o httpget
 *	          or: cc httpget.c -o httpget
 *               (add -lnsl -lsocket on Solaris)
 *
 * USAGE
 *   Usage: httpget [options] <URL>
 *  options:       -x <host>   Use proxy.
 *                             Default port is 1080.
 *                 -p <port>   Use port other than default
 *                             for current protocol.
 *
 *   Example: httpget -p 8080 -x my-proxy.my.site http://www.from.here/
 *
 * PROJECT
 *   Initial author and project maintainer.
 *   Rafael Sagula - sagula@inf.ufrgs.br
 *	             http://www.inf.ufrgs.br/~sagula
 *
 * BIBLIOGRAPHY
 *   - "UNIX Network Programming" - Richard Stevens (rstevens@noao.edu)
 *   - "Advanced Programming in UNIX Environment" Richard Stevens (the same)
 *   - RFC 1738 T. Berners-Lee, L. Masinter, M. McCahill,
 *     "Uniform Resource Locators (URL)", 12/20/1994
 *
 * HISTORY
 *   Daniel Stenberg <Daniel.Stenberg@sth.frontec.se>
 *   - Adjusted it slightly to accept named hosts on the command line. We
 *     wouldn't wanna use IP numbers for the rest of our lifes, would we?
 *
 *   Bjorn Reese <breese@imada.ou.dk>
 *   - Implemented URLs (and skipped the old syntax).
 *   - Output is written to stdout, so to achieve the above example, do:
 *       httpget http://143.54.10.6/info_logo.gif > test.gif
 *
 *   Johan Andersson <johan@homemail.com>
 *   - Implemented HTTP proxy support.
 *   - Receive byte counter added.
 *
 *   Daniel Stenberg <Daniel.Stenberg@sth.frontec.se>
 *   - Bugfixed the proxy usage. It should *NOT* use nor strip the port number
 *     from the URL but simply pass that information to the proxy. This also
 *     made the user/password fields possible to use in proxy [ftp-] URLs.
 *     (like in ftp://user:password@ftp.my.site:8021/README)
 *
 *   Rafael Sagula <sagula@inf.ufrgs.br>
 *   - Let "-p" before "-x".
 *
 *   Johan Andersson
 *   - Discovered and fixed the problem with getting binary files. puts() is
 *     now replaced with fwrite(). (Daniel's note: this also fixed the buffer
 *     overwrite problem I found in the previous version.)
 *
 *   Daniel Stenberg
 *   - Well, I added a lame text about the time it took to get the data. I also
 *     fought against Johan to prevent his -f option (to specify a file name
 *     that should be written instead of stdout)! =)
 *   - Made it write 'connection refused' for that particular connect()
 *     problem.
 *   - Renumbered the version. Let's not make silly 1.0.X versions, this is
 *     a plain 1.3 instead.
 */


 #include <stdio.h>
 #include <string.h>
 #include <ctype.h>
 #include <sys/types.h>
 #include <sys/stat.h>
 #include <sys/socket.h>
 #include <netinet/in.h>
 #include <arpa/inet.h>
 #include <sys/time.h>
 #include <sys/resource.h>
 #include <unistd.h>
 #include <fcntl.h>
 
 #include <netdb.h>
 
 #include <errno.h>
 
 #define BUFSIZE 2048
 
 int readline(int fd,char *ptr,int maxlen);
 int send_get(int fd, char *file);
 void skip_header(int fd);
 
 /* Stolen from Dancer source code: */
 #ifndef INADDR_NONE
 #define INADDR_NONE -1
 #endif
 struct hostent *GetHost(char *hostname)
 {
   struct hostent *h = NULL;
   unsigned long in;
 
   if ( (in=inet_addr(hostname)) != INADDR_NONE ) {
     if ( (h=gethostbyaddr((char *)&in, sizeof(in), AF_INET)) == NULL )
       fprintf(stderr, "gethostbyaddr(2) failed for %s\n", hostname);
   } else if ( (h=gethostbyname(hostname)) == NULL ) {
     fprintf(stderr, "gethostbyname(2) failed for %s\n", hostname);
   }
   return (h);
 }
 
 #define CONF_DEFAULT 0
 #define CONF_PROXY   1
 #define CONF_PORT    2
 
 /*
  * httpget <url>
  * (result put on stdout)
  *
  * <url> ::= <proto> "://" <host> [ ":" <port> ] "/" <path>
  *
  * <proto> = "HTTP" | "GOPHER"
  */
 
 void main(argc,argv)
     int argc;
     char *argv[];
 {
   struct hostent *hp=NULL;
   int sockfd, addrlen, clilen, nread, port, i;
   struct sockaddr_in *my_addr, serv_addr, cli_addr;
   char buf[BUFSIZE+1];
   char host_addr[100];
   char proxy_name[100];
   char proto[64];
   char name[256];
   char path[512];
   char *ppath, *tmp;
   int defport;
   int conf;
   long bytecount;
   time_t now;
 
   conf = CONF_DEFAULT;
 
   if (argc < 2) {
     fprintf(stderr, "HttpGet v1.3\n - Rafael Sagula/Daniel Stenberg/Bjorn Reese/Johan Andersson - 1996-97\n");
     fprintf(stderr, "usage: httpget [options...] <url>\n");
     fprintf(stderr, "options:        -x <host>   Use proxy.\n");
     fprintf(stderr, "                            Default port is 1080.\n");
     fprintf(stderr, "                -p <port>   Use port other than default\n");
     fprintf(stderr, "                            for current protocol.\n");
     exit(-1);
   }
 
 
   /* Parse <url> */
   if (3 != sscanf(argv[argc - 1], "%64[^\n:]://%256[^\n/]%512[^\n]", proto, name, path)) {
     fprintf(stderr, "<url> malformed.\n");
     exit(-1);
   }
 
   if (!strcasecmp(proto, "HTTP"))
   {
     defport = 80;
   }
   else
   if (!strcasecmp(proto, "GOPHER"))
   {
     defport = 70;
     /* Skip /<item-type>/ in path if present */
     if (isdigit(ppath[1]))
     {
       ppath = strchr(&path[1], '/');
       if (ppath == NULL)
       ppath = path;
     }
   }
 
 
   /* Parse options */
   for (i = 1; i < argc-1; i++)
   {
     if (!strcmp(argv[i], "-p"))
     {
       defport = atoi(argv[++i]);
       conf |= CONF_PORT;
     }
     else if (!strcmp(argv[i], "-x"))
     {
       strcpy(proxy_name, argv[++i]);
       conf |= CONF_PROXY;
       /* Sagula:
        * If port defined, don't change it. */
       if (!(conf & (CONF_PORT))) {defport = 1080;}
       strcpy(path, argv[argc - 1]);
     }
   }
 
   ppath = path;
 
   alarm(60*30); /* 30 minutes */
 
   if (conf & CONF_PROXY) {
     /* Daniel:
      * When using a proxy, we shouldn't extract the port number from the 
      * URL since that would destroy it. */
      
     if(!(hp = GetHost(proxy_name))) {
       fprintf(stderr, "Couldn't resolv '%s', exiting.\n", proxy_name);
       exit(-1);
     }
     port = defport;
   } 
   else {
     tmp = strchr(name, ':');
     if (tmp == NULL) {
       port = defport;
     } 
     else {
       *tmp++ = '\0';
       port = atoi(tmp);
     }
 
     if(!(hp = GetHost(name))) {
       fprintf(stderr, "Couldn't resolv '%s', exiting.\n", name);
       exit(-1);
     }
   }
 
   now = time(NULL);
    
   sockfd = socket(AF_INET, SOCK_STREAM, 0);
 
   memset((char *) &serv_addr, '\0', sizeof(serv_addr));
   memcpy((char *)&(serv_addr.sin_addr), hp->h_addr, hp->h_length);
   serv_addr.sin_family = hp->h_addrtype;
   serv_addr.sin_port = htons(port);
   
   if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
     switch(errno) {
     case ECONNREFUSED:
       fprintf(stderr, "Connection refused.\n");
       break;
 
     default:
       fprintf(stderr, "Can't connect to server.\n");
       break;
     }
     exit(-1);
   };
   fprintf(stderr, "Connected!\n");
 
   send_get(sockfd, path);
 
   skip_header(sockfd);	
 
   bytecount = 0;
 
   for (;;) {
     nread = read(sockfd, buf, BUFSIZE);
     bytecount += nread;
     if (nread==0) {
       time_t end=time(NULL);
       close(sockfd);
       fprintf(stderr, "\n%i bytes received in %d seconds (%d bytes/sec).\n",
           bytecount, end-now, bytecount/(end-now?end-now:1));
       exit(0);
     }
     alarm(60*30); /* 30 minutes */
     fwrite(buf, 1, nread, stdout);
   }
 }
 
 
 int readline(int fd,char *ptr,int maxlen)
 {
   int n, rc;
   char c;
         
   for (n = 1; n < maxlen; n++) {
     if ( (rc = read(fd, &c, 1)) == 1) {
       if (c == '\n')
     break;
       *ptr++ = c;
     } 
     else 
       if (rc == 0) {
     if (n == 1)
       return(0);
     else
       break;
       } 
       else
     return(-1);
   }
   *ptr = 0;
   return(n);
 } 
 
 int send_get(int fd, char *file)
 {
   char s[512];
 
   sprintf(s, "GET %s\015\012Pragma: no-cache\015\012", file);
   return(write(fd, s, strlen(s)));
 }
 
 void skip_header(int fd)
 {
   char s[256];
   int nr = 3;
 
   while ((nr>2)) {
     nr=readline(fd, s, 256);
   }
 }