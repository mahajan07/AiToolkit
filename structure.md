# CODES
## Calculate Time, hours, minutes, time:-
def calculate_time(s_time):
    seconds = np.round(time.time() - s_time, 0)
    minutes = hours = 0

    # claculate minutes
    while seconds > 60:
        minutes += 1
        seconds -= 60
    
    # calculate hours
    while minutes > 60:
        hours += 1
        minutes -= 60

    print(f'Cell Executed in {hours}h {minutes}m {seconds}s')
