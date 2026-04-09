#!/bin/bash                                                                                                                        
# status.sh — check LSEG download progress                                                                                       
cd "/home/datafeed/market data library/lseg data fetch/equity_options"                                                             
                                                                                                                                   
BARS=$(ps aux | grep "download_om_minute_bars" | grep -v grep | awk '{print $13}')                                                 
TRADES=$(ps aux | grep "download_trades" | grep -v grep | awk '{print $13}')                                                       
                                                                                                                                   
echo "=== BARS: $BARS ==="                                                                                                         
tail -2 "data/$BARS/om_run.log" 2>/dev/null                                                                                        
echo ""                                                                                                                          
echo "=== TRADES: $TRADES ==="
tail -2 "data/$TRADES/trades_run.log" 2>/dev/null                                                                                  
echo ""
echo "Completed bars:   $(grep -l 'COMPLETE' data/*/om_run.log 2>/dev/null | wc -l) / 6570"                                        
echo "Completed trades: $(grep -l 'COMPLETE' data/*/trades_run.log 2>/dev/null | wc -l) / 6570"                                    
echo ""                                                                                                                            
du -sh data/                                                                                                                       
df -h /media/datafeed/Expansion | tail -1  
