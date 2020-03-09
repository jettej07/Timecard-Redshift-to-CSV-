import psycopg2
import pandas as pd
import datetime, sys, os, time


class Redshift:
    def __init__(self, start_date, end_date, user, pw, campaigns):
        print('Connecting to redshift...')
        try:
            self.conn = psycopg2.connect(
                host='<<< HOST HERE >>>',
                dbname='dev',
                port='5439',
                user=user,
                password=pw)
        except:
            if os.path.isfile('creds.txt'):
                os.remove('creds.txt')
            print('\nInvalid credentials. Closing the program in 3 seconds...\n')
            time.sleep(3)
            sys.exit()
        self.start_date = start_date
        self.end_date = end_date
        self.cur = self.conn.cursor()
        self.campaigns = campaigns
        self.campaigns_not_found = []
        self.campaigns_found = []


    def get_data(self):
        headers = ['campaign', 'date', 'eid', 'name', 'tl', 'timewarp_login',
                   'timewarp_logout', 'teleopti_schedule_start', 'rest_day_flag',
                   'unauthorized_absence', 'authorized_absence', 'site']

        for campaign in self.campaigns:
            campaign = campaign.strip()
            sql = f"""
                SELECT 
                    campaign_grp,
                    date,
                    eid,
                    name,
                    tl,
                    timewarp_login,
                    timewarp_logout,
                    teleopti_schedule_start,
                    rest_day_flag,
                    unauthorized_absence,
                    authorized_absence,
                    site
                FROM public.v_utl_teleopti_billing_last_3_months
                WHERE date >= '{self.start_date}' AND date <= '{self.end_date}' AND campaign_grp ILIKE '%{campaign}%';
            """
            print(f'Fetching data for {campaign}...')
            self.cur.execute(sql)
            data = self.cur.fetchall()
            if len(data) ==0:
                self.campaigns_not_found.append(campaign)
                continue
            print('Transforming data...')
            df = pd.DataFrame(data, columns=headers)
            df['unauthorized_absence'].fillna('', inplace=True)
            df['authorized_absence'].fillna('', inplace=True)
            df['remarks'] = df['unauthorized_absence'] + df['authorized_absence']
            print('Parsing dates...')
            df['timewarp_login'] = df['timewarp_login'].astype('datetime64')
            df['timewarp_logout'] = df['timewarp_logout'].astype('datetime64')
            df['teleopti_schedule_start'] = df['teleopti_schedule_start'].astype('datetime64')
            df['status'] = df.apply(self.get_status, axis=1)
            df.pop('rest_day_flag')
            cols = ['campaign', 'date','eid','name','tl','timewarp_login','timewarp_logout','status','teleopti_schedule_start', 'remarks', 'site']
            print('Filtering and re-arranging columns...')
            df = df[cols]
            print('Sorting values...')
            df.sort_values(by=['campaign', 'eid', 'date'], inplace=True)
            print('Transforming dates to time...')
            df['timewarp_login'] = df['timewarp_login'].dt.strftime('%H:%M:%S')
            df['timewarp_logout'] = df['timewarp_logout'].dt.strftime('%H:%M:%S')
            df['teleopti_schedule_start'] = df['teleopti_schedule_start'].dt.strftime('%H:%M:%S')
            df.replace({'NaT': None}, inplace=True)
            print('Converting to csv file...')
            df.to_csv('ConsolidatedTC.csv', mode='a', index=False, header=False, encoding='utf-8')
            self.campaigns_found.append(campaign)

    def close_connection(self):
        print('Closing connection...')
        self.conn.close()

    @staticmethod
    def get_status(df):
        if df['rest_day_flag'] == 'Rest Day':
            val = 'Rest Day'
        elif pd.isna(df['timewarp_login']) and pd.isna(df['timewarp_logout']) and (pd.isna(df['remarks']) or df['remarks'] == ''):
            val = 'Missing LI/LO'
        elif pd.isna(df['timewarp_login']) and pd.isna(df['timewarp_logout']) and (not pd.isna(df['remarks']) or df['remarks'] != ''):
            val = 'Absent'
        elif (pd.isna(df['timewarp_login']) or df['timewarp_login'] == ''):
            val = 'No Login'
        elif not pd.isna(df['timewarp_login']) or df['timewarp_login'] != '':
            if df['timewarp_logout'] == '' or pd.isna(df['timewarp_logout']):
                val = 'No Logout'
            elif df['timewarp_login'] >= df['teleopti_schedule_start'] + datetime.timedelta(minutes=1):
                val = 'Late'
            else:
                val = 'On Time'
        else: val = ''
        return val


def test_connection(user, pw):
    try:
        print('Validating credentials...')
        conn = psycopg2.connect(
            host='tku-redshift.cl7hlvwdi0q8.ap-southeast-1.'
                'redshift.amazonaws.com',
            dbname='dev',
            port='5439',
            user=user,
            password=pw)
        conn.close()
        print('Credentials validated...')
        return None
    except:
        if os.path.isfile('creds.txt'):
            os.remove('creds.txt')
        print('\nInvalid credentials. Closing the program in 3 seconds...\n')
        time.sleep(3)
        sys.exit()
