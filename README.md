# LDB

## AWS Performance

`
ubuntu@ip-172-31-13-201:~/bigo$ cat wrk.log Running 180m test @ http://172.31.46.10:8080/
4 threads and 100 connections Thread Stats   Avg      Stdev     Max +/- Stdev Latency     5.88ms    6.04ms 247.03ms   89.52% Req/Sec     5.29k   740.53     8.04k    66.77% Latency Distribution 50% 3.96ms 75% 6.87ms 90% 12.29ms 99% 31.42ms 158534168 requests in 125.51m, 17.13GB read Requests/sec:  21051.67 Transfer/sec:      2.33MB total requests: 158534168
`
## building

    mvn clean package

