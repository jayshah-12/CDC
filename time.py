from datetime import datetime, timedelta 
from pyperclip import paste

time_fmt = "%I:%M:%S %p"
now = datetime.now()

# # Read from file 
# with open("sample_input.txt", "r") as f:
#     times = [line.strip() for line in f.read().strip().split('\n')]

# Read from clipboard
times = [line.strip() for line in paste().strip().split()]
times = [' '.join(times[i:i+2]) for i in range(0, len(times), 2)]

in_times = times[0::2]
out_times = times[1::2]
if out_times[-1] == "MISSING":
    out_times[-1] = datetime.now().replace(year=1900, month=1, day=1).strftime(time_fmt)

in_times = [datetime.strptime(in_time, time_fmt) for in_time in in_times]
out_times = [datetime.strptime(out_time, time_fmt) for out_time in out_times]
print()

gross_hours = out_times[-1] - in_times[0]
print("Your Current Gross Hours", gross_hours, end="\n\n")

eff_hours = timedelta(hours=0)
for in_time, out_time in zip(in_times, out_times):
    eff_hours += out_time-in_time
print("Your Current Effective Hours:", eff_hours, end="\n\n")

break_hours = gross_hours - eff_hours
print("Break Hours", break_hours, end="\n\n")


time_remaining = timedelta(hours=9) - eff_hours
if time_remaining.total_seconds() < 0 :
    print("No Time Remaining\n LEAVE NOW !!!")
    exit()
else:
    print("Time Remaining", time_remaining, end="\n\n")
time_completing = out_times[-1] + time_remaining
print("Need to leave at", time_completing.strftime(time_fmt), end="\n\n")