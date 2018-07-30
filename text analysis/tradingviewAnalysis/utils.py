def cryptocurrenciesAbbrev(cryptocurrenciesList):
    cryptocurrencies = {
        'Bitcoin' : 'btc',
        'Litecoin' : 'ltc',
        'U.S. Dollar' : 'usd',
        'EOS' : 'eos',
        'Bitcoin Cash' : 'bch',
        'Tether' : 'usdt',
        'Ripple' : 'xrp',
        'Vertcoin' : 'vtc',
        'Dogecoin' : 'doge',
        'Euro' : 'eur'
    }
    return [cryptocurrencies[i] for i in cryptocurrenciesList if i in cryptocurrencies.keys()]