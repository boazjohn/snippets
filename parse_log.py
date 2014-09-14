import re
import math
import datetime

def Main():
    ip_req = {}
    req_ten = {}
    resp_time = []
    j = 0
    with open('access.log', 'r') as f:
        for line in f:
            pat = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
            ip = re.findall(pat, line)
            patx = re.compile("\[.*.\]")
            tim = re.findall(patx, line)[0]
            tim = tim.split(" ")
            if j == 0:
                ptime = datetime.datetime.strptime(tim[0][1:],\
                                                   '%d/%b/%Y:%H:%M:%S')
                j += 1

            timen = datetime.datetime.strptime(tim[0][1:],\
                                               '%d/%b/%Y:%H:%M:%S')
            delta = str(ptime) + ' - ' + str(ptime + datetime.\
                            timedelta(minutes = 10))
            if timen <= (ptime + datetime.timedelta(minutes = 10)):

                req_ten[delta] = req_ten.get(delta, 0) + 1
            else:
                req_ten[delta] = req_ten.get(delta, 0)
                ptime += datetime.timedelta(minutes = 10)
            
            vals = line.split(" ")
            # print (vals[8] + ' ' + vals[9])
            resp_time.append(int(vals[8]))            

            ip_req[ip[0]] = ip_req.get(ip[0], 0) + 1
            
    print('------IP------\t----Requests----') 
    for ips in sorted(ip_req):
        print("%-15s\t\t%d" % (ips, ip_req[ips]))
  
    print('\n-------------------Date------------------'\
          '\t---Requests---')
    for req in sorted(req_ten):
        print('%s\t\t%d' % (req, req_ten[req])) 
    
    resp_time.sort()
    indx = int(math.ceil(len(resp_time) * .99))
    print('\nTP99 = ' + str(resp_time[indx]))          
    
    
if __name__ == "__main__":
    Main()
