## on server @ {{PROVIDER}}
{{IP}} {{PORT}} (external ssh port)
payload: ~10000 on port 80/443
payload: 1292 (+16 for headers) on higher ports
fling_port=19001
sync_port=19002
conns=12

## on phone
start-tcs-client
`ssh root@localhost -p 6000`
