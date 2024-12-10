import rt_get_ticket
import datetime
import json
import copy

def main(FromDate='2015-07-01', ToDate='2024-10-01'):
    train_dataset, eval_dataset = [], []
    First = 'True'
    From = FromDate
    To = From
    date = datetime.date.fromisoformat(FromDate)
    while To != ToDate:
        date = datetime.date.fromisoformat(From)
        date = date + datetime.timedelta(days=90)
        To = date.strftime('%Y-%m-%d')
        if To > ToDate:
            To = ToDate
        print(From, To)
        cur_train_dataset, cur_eval_dataset = rt_get_ticket.main(FromDate=From, ToDate=To, First=First)
        train_dataset.extend(copy.deepcopy(cur_train_dataset))
        eval_dataset.extend(copy.deepcopy(cur_eval_dataset))
        From = date - datetime.timedelta(days=1)
        From = From.strftime('%Y-%m-%d')
        First = 'False'
        
    with open("/data/24ds_train_ascii.json", 'w') as f:
        json.dump(train_dataset, f)
    with open("/data/24ds_eval_ascii.json", 'w') as f:
        json.dump(eval_dataset, f)
    

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Put in date range to process')
    parser.add_argument('--FromDate', help='Earliest create date, 2020-01-01', default='2015-07-01')
    parser.add_argument('--ToDate', help='Latest create date, 2021-01-01', default='2024-10-01')
    args = parser.parse_args()
    main(FromDate=args.FromDate, ToDate=args.ToDate)