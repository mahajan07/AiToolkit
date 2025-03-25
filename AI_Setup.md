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

# Profile report of your data

df = pd.read_csv(datafile_you_want_to_use, nrows=_choose_)
profile = pandas_profiling.ProfileReport(df_temp, title="Profile Report", minimal=True, progress_bar=False)
profile.to_notebook_iframe()
