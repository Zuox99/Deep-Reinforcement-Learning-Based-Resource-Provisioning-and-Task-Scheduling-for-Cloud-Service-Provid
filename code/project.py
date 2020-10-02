import os

#pre-process file
cmd = 'python preprocess.py'
r = os.popen(cmd)
r.close()

#get task infor
cmd = './userWorkload input.txt output_20000.txt'
r = os.popen(cmd)
r.close()

#get environment-RR result
cmd = 'python env_RR.py'
r = os.popen(cmd)
r.close()

#get environment-dqn result
cmd = 'python env_dqn.py'
r = os.popen(cmd)
r.close()

#get improved-environment-rr result
cmd = 'python improved_env_rr.py'
r = os.popen(cmd)
r.close()

# #get result of RR
# outfile = open("result.txt", 'w')    
# outfile.write(str(r.read()) + "\n")
# r.close()
# outfile.close()