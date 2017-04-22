def continuous_contracts_minexpiry():
    """
    Create continuous contracts file by selecting minimum available expiry
    :return: None, create continuous contract files
    """

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    path = MINEXP_CONTINUOUS

    utils.mkdir(path)

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()

    e = read_expiry_hist()
    print(e)

    exphist = e['expiry_dates']

    curr_expiry = {}
    exp_idx = {}
    exp_rollover = {}
    rollover_multiplier = {}
    success, error = 0, 0
    for file in csv_files:
        try:
            date = file[0:10]
            df = pd.read_csv(file)
            date_pd = pd.DataFrame()

            for symbol in df['Symbol'].unique():

                if symbol not in prev_expiry:
                    curr_expiry[symbol] = exphist[symbol][0]  # Initialize
                    exp_rollover[symbol] = False  # Initialize
                    rollover_multiplier[str.strip(symbol)] = {}  # Initialize

                all_records = df.loc[df['Symbol'] == symbol]
                curr_exp_record = all_records.loc[all_records['Expiry Date'] == curr_expiry[symbol]]
                if curr_exp_record.empty:
                    sel_record = any_record.loc[any_record['Expiry Date'] == min(any_record['Expiry Date'])]
                else:
                    sel_record = prev_exp_record

                date_pd = pd.concat([date_pd, sel_record], axis=0)

                if exp_idx[symbol] == -1:  # First record for symbol
                    sel_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][0])]
                    exp_idx[symbol] = 0
                    if symbol in debug:
                        print('@@@1', date, symbol)
                elif exp_idx[symbol] == len(exphist[symbol]) - 1:  # Last expiry for symbol
                    sel_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                    if symbol in debug:
                        print('@@@2', date, symbol)
                else:
                    curr_record = df.loc[
                        (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                    nxt_record = df.loc[(df['Symbol'] == symbol)
                                        & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol] + 1])]
                    if symbol in debug:
                        print('@@@3', date, symbol)

                    if not curr_record.empty and not nxt_record.empty:
                        if symbol in debug:
                            print('@@@4', date, symbol)
                        if exp_rollover[symbol]:
                            sel_record = curr_record
                            exp_rollover[symbol] = False
                            if symbol in debug:
                                print('@@@5', date, symbol)
                        else:
                            sel_record = curr_record
                            if symbol in debug:
                                print('@@@6', date, symbol)
                        if int(curr_record[parm]) < int(nxt_record[parm]):
                            exp_rollover[symbol] = True
                            exp_idx[symbol] += 1
                            rollover_multiplier[str.strip(symbol)][date] = curr_record['Close'].iloc[0] / \
                                                                           nxt_record['Close'].iloc[0]
                            if symbol in debug:
                                print('@@@7', date, symbol)
                    elif curr_record.empty and nxt_record.empty:
                        any_record = df.loc[df['Symbol'] == symbol]
                        min_exp_record = any_record.loc[any_record['Expiry Date'] == min(any_record['Expiry Date'])]
                        if symbol in debug:
                            print('@@@8', date, symbol, '$$$$$', len(any_record.index),
                                  min_exp_record['Expiry Date'].to_string())
                            # print(csv_files[csv_files.index(file) - 1])
                            # pass
                    elif curr_record.empty:
                        sel_record = nxt_record
                        exp_idx[symbol] += 1
                        rollover_multiplier[str.strip(symbol)][date] = 1
                        if symbol in debug:
                            print('@@@9', date, symbol)
                    elif nxt_record.empty:
                        sel_record = curr_record
                        if symbol in debug:
                            print('@@@10', date, symbol)

                if not sel_record.empty:
                    date_pd = pd.concat([date_pd, sel_record], axis=0)

            date_pd['Symbol'] = date_pd['Symbol'].apply(str.strip)
            date_pd.to_csv('{}{}'.format(path, file), sep=',', index=False)
            print(date, ',Continuous contract created', file)
            success += 1

        except:
            print(date, ',Error creating Continuous contract', file)
            error += 1

    with open(path + ROLLOVER_MULT, 'wb') as handle:
        pkl.dump(rollover_multiplier, handle)

    print('Contract created for {} days, {} errors'.format(success, error))


def continuous_contracts_vol_oi_rollover_debug(parm, debug=None):
    """
    Create continuous contracts file on volume or oi rollover, create multipliers history
    :param parm: 'Volume' or 'Open Interest'
    :return: None, create continuous contract files
    """

    if debug is None:
        debug = []

    csv_files = [f for f in os.listdir(os.curdir) if f.endswith('.csv')]
    csv_files.sort()

    if parm == 'Volume':
        path = VOL_CONTINUOUS
    elif parm == 'Open Interest':
        path = OI_CONTINUOUS

    utils.mkdir(path)

    if not os.path.isfile(EXPIRIES):
        write_expiry_hist()

    e = read_expiry_hist()
    print(e)

    exphist = e['expiry_dates']

    exp_idx = {}
    exp_rollover = {}
    rollover_multiplier = {}
    success, error = 0, 0
    for file in csv_files:
        #try:
        date = file[0:10]
        df = pd.read_csv(file)
        date_pd = pd.DataFrame()
        for symbol in df['Symbol'].unique():
            # print(symbol, debug)
            if symbol in debug:
                print('@@@0', date, symbol)
            if symbol not in exp_idx:
                exp_idx[symbol] = -1  # Initialize
                exp_rollover[symbol] = False  # Initialize
                rollover_multiplier[str.strip(symbol)] = {}  # Initialize

            if exp_idx[symbol] == -1:  # First record for symbol
                sel_record = df.loc[(df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][0])]
                exp_idx[symbol] = 0
                if symbol in debug:
                    print('@@@1', date, symbol)
            elif exp_idx[symbol] == len(exphist[symbol]) - 1:  # Last expiry for symbol
                sel_record = df.loc[
                    (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                if symbol in debug:
                    print('@@@2', date, symbol)
            else:
                curr_record = df.loc[
                    (df['Symbol'] == symbol) & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol]])]
                nxt_record = df.loc[(df['Symbol'] == symbol)
                                    & (df['Expiry Date'] == exphist[symbol][exp_idx[symbol] + 1])]
                if symbol in debug:
                    print('@@@3', date, symbol)

                if not curr_record.empty and not nxt_record.empty:
                    if symbol in debug:
                        print('@@@4', date, symbol)
                    if exp_rollover[symbol]:
                        sel_record = curr_record
                        exp_rollover[symbol] = False
                        if symbol in debug:
                            print('@@@5', date, symbol)
                    else:
                        sel_record = curr_record
                        if symbol in debug:
                            print('@@@6', date, symbol)
                    if int(curr_record[parm]) < int(nxt_record[parm]):
                        exp_rollover[symbol] = True
                        exp_idx[symbol] += 1
                        rollover_multiplier[str.strip(symbol)][date] = curr_record['Close'].iloc[0] / \
                                                                       nxt_record['Close'].iloc[0]
                        if symbol in debug:
                            print('@@@7', date, symbol)
                elif curr_record.empty:
                    # elif curr_record.empty and nxt_record.empty:
                    any_record = df.loc[df['Symbol'] == symbol]
                    sel_record = any_record.loc[any_record['Expiry Date'] == min(any_record['Expiry Date'])]
                    if sel_record['Expiry Date'].to_string()[-10:] >= date
                    exp_idx[symbol] = exphist[symbol].index(sel_record['Expiry Date'].to_string()[-10:])
                    rollover_multiplier[symbol][date] = 1
                    if symbol in debug:
                        print('@@@8', date, symbol, '$$$$$', len(any_record.index),
                              sel_record['Expiry Date'].to_string()[-10:],
                              exphist[symbol].index(sel_record['Expiry Date'].to_string()[-10:]),
                              exphist[symbol][exp_idx[symbol]],
                              exphist[symbol],
                              sel_record
                              )
                        # print(csv_files[csv_files.index(file) - 1])
                        # pass
                #elif curr_record.empty:
                #    sel_record = nxt_record
                #    exp_idx[symbol] += 1
                #    rollover_multiplier[str.strip(symbol)][date] = 1
                #    if symbol in debug:
                #        print('@@@9', date, symbol)
                elif nxt_record.empty:
                    sel_record = curr_record
                    if symbol in debug:
                        print('@@@10', date, symbol)

            if not sel_record.empty:
                date_pd = pd.concat([date_pd, sel_record], axis=0)

        date_pd['Symbol'] = date_pd['Symbol'].apply(str.strip)
        date_pd.to_csv('{}{}'.format(path, file), sep=',', index=False)
        print(date, ',Continuous contract created', file)
        success += 1

        #except:
        print(date, ',Error creating Continuous contract', file)
        error += 1

    with open(path + ROLLOVER_MULT, 'wb') as handle:
        pkl.dump(rollover_multiplier, handle)

    print('Contract created for {} days, {} errors'.format(success, error))