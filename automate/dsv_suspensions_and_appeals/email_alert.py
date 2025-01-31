from my_functions import get_config, get_data, clean_data, process, send_log
from datetime import datetime

def main():
    config = get_config('monthly_alert_configs.json')
    df = get_data(config['query'])
    df = clean_data(df)
    # send email alerts
    df['Comments'] = df.apply(process, axis=1, args=(config,))
    # send run log
    send_log(df, config['log_config'])

if __name__ == "__main__":
    start_time = datetime.now()
    print("start:", str(start_time))

    main()

    end_time = datetime.now()
    print("end:", str(end_time))
    run_duration = str(end_time - start_time).split('.')[0].split(":")
    print("\nRun Duration: "+run_duration[1]+'m '+run_duration[2]+'s')
