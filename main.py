from database import Redshift, test_connection
import datetime as dt
import sys, os, base64, time


def close_program():
    print("\nInvalid entry after 3 attempts. Closing the program...")
    time.sleep(3)
    sys.exit()


def validate(date):
    try:
        dt.datetime.strptime(date, '%Y-%m-%d')
        return True
    except:
        print('Invalid date. Please follow date format yyyy-mm-dd. \n')
        return False

def get_input():
    if os.path.isfile('creds.txt'):
        with open('creds.txt', 'r') as f:
            line = f.read().split()
            usrname = line[0].encode('utf-8')
            psword = line[1].encode('utf-8')
            
            usrname = base64.b64decode(usrname).decode('utf-8')
            psword = base64.b64decode(psword).decode('utf-8')

            test_connection(usrname, psword)

    else:
        usrname = input('Enter your redshift username:\n')
        encoded_usrname = usrname.encode('utf-8')
        psword = input('\nEnter your redshift password:\n')
        encoded_psword = psword.encode('utf-8')
        test_connection(usrname, psword)

        save_creds = input('\nSave username and password? [Y/N]  ')
        if save_creds.lower() == 'y':
            with open('creds.txt', 'w') as f:
                encoded_usrname = base64.b64encode(encoded_usrname).decode('utf-8')
                encoded_psword = base64.b64encode(encoded_psword).decode('utf-8')
                f.write(f'{encoded_usrname}\t{encoded_psword}')

    counter = 0
    while counter < 4:
        counter += 1
        start_date = input('\nEnter start date (yyyy-mm-dd): ')
        if validate(start_date):
            counter = 0
            break
        else:
            if counter == 4:
                close_program()
            continue
    while counter < 4:
        counter += 1
        end_date = input('\nEnter end date (yyyy-mm-dd): ')
        if validate(end_date):
            counter = 0
            break
        else:
            if counter == 4:
                close_program()
            continue
    while counter < 4:
        counter += 1
        campaign = input('\nEnter campaign. If multiple campaigns, use commas to separate them.\n').split(',')
        if counter == 4:
            close_program()
        elif campaign[0] == '':
            continue
        else:
            break
    params = [start_date, end_date, usrname, psword, campaign]
    return params


if __name__ == "__main__":
    try:
        with open('ConsolidatedTC.csv', 'w') as f:
            headers = ['campaign', 'date','eid','name','tl','timewarp_login','timewarp_logout','status','teleopti_schedule_start', 'remarks', 'site']
            f.write(','.join(headers) + '\n')
    except PermissionError:
        print('\nERROR FOUND: Permission denied: The file "ConsolidatedTC.csv" is currently open. Please close it first.\n')
        any_key = input('Press "ENTER" to exit the program.\n\n')
        if any_key:
            sys.exit()
    params = get_input()
    rs = Redshift(*params)
    rs.get_data()
    rs.close_connection()
    print('\n=================================================================================\n')
    print('Program Complete!\n')
    print(f'Extracted {len(rs.campaigns_found)} out of {len(rs.campaigns)} campaigns.\n')
    if rs.campaigns_found:
        print(f'Campaign(s) found:\n{rs.campaigns_found}\n')
    if rs.campaigns_not_found:
        print(f'Campaign(s) NOT found:\n{rs.campaigns_not_found}... Please verify the campaign name as it appears in redshift.\n\n')
    any_key = input('Press "ENTER" to exit the program.\n\n')
    if any_key:
        sys.exit()
