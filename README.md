# pivx-scripts
pivx-scripts

1. cd stats

2. python3 -m venv .venv

3. source .venv/bin/activate

4. pip3 install -r requirements.txt

5. update default json and db paths in update_stats_addresses_earnings.py update_stats_addresses_balances.py

6. python3 update_stats_addresses_earnings.py

7. python3 update_stats_addresses_balances.py

Note:

Result is json file of the sample format below
(block_counters array structure is index = 0 thru 4 staking counts for last 50 250 500 750 1000 blocks respectively 

```json (rewards)
[
  {
    "address": "D6ixy2j2qyYMPAibobymDkiXMEYSyKX3Mb",
    "counters": {
      "block_counters": [
        22,
        44,
        44,
        44,
        44
      ],
      "date_counters": {
        "ymdH": [
          {
            "date": "2021-10-24 08",
            "count": 4
          }
        ],
        "ymd": [
          {
            "date": "2021-10-24",
            "count": 4
          }
        ],
        "ym": [
          {
            "date": "2021-10",
            "count": 4
          }
        ],
        "y": [
          {
            "date": "2021",
            "count": 4
          }
        ]
      }
    }
  },
  {
    "address": "STx39nArrm6fRBuo1QGm76Aax9YURGCiYi",
    "counters": {
      "block_counters": [
        12,
        26,
        26,
        26,
        26
      ],
      "date_counters": {
        "ymdH": [
          {
            "date": "2021-10-24 08",
            "count": 2
          }
        ],
        "ymd": [
          {
            "date": "2021-10-24",
            "count": 2
          }
        ],
        "ym": [
          {
            "date": "2021-10",
            "count": 2
          }
        ],
        "y": [
          {
            "date": "2021",
            "count": 2
          }
        ]
      }
    }
  },
  {
    "address": "DBa5cB3hMns5kwrdWVuCx9JjR5s5sVCY7U",
    "counters": {
      "block_counters": [
        14,
        22,
        22,
        22,
        22
      ],
      "date_counters": {
        "ymdH": [
          {
            "date": "2021-10-24 08",
            "count": 6
          }
        ],
        "ymd": [
          {
            "date": "2021-10-24",
            "count": 6
          }
        ],
        "ym": [
          {
            "date": "2021-10",
            "count": 6
          }
        ],
        "y": [
          {
            "date": "2021",
            "count": 6
          }
        ]
      }
    }
  },
  {
    "address": "DDU6BCfxp2eGdQ5AuoyL4QQo6D4abms5qg",
    "counters": {
      "block_counters": [
        4,
        12,
        12,
        12,
        12
      ]
    }
  },
  {
    "address": "DQqkQD2s4CgG2GAjcRXiT5crpXneLGHHVT",
    "counters": {
      "block_counters": [
        8,
        8,
        8,
        8,
        8
      ]
    }
  },
  {
    "address": "D6B4Sw89gsKveLEdG9G5GC49APq6VJdMi6",
    "counters": {
      "block_counters": [
        2,
        4,
        4,
        4,
        4
      ],
      "date_counters": {
        "ymdH": [
          {
            "date": "2021-10-24 08",
            "count": 2
          }
        ],
        "ymd": [
          {
            "date": "2021-10-24",
            "count": 2
          }
        ],
        "ym": [
          {
            "date": "2021-10",
            "count": 2
          }
        ],
        "y": [
          {
            "date": "2021",
            "count": 2
          }
        ]
      }
    }
  },
  {
    "address": "D8xM8qYTxCngeWeMTzC5b5aRCMkAmLJUfk",
    "counters": {
      "block_counters": [
        2,
        4,
        4,
        4,
        4
      ],
      "date_counters": {
        "ymdH": [
          {
            "date": "2021-10-24 08",
            "count": 2
          }
        ],
        "ymd": [
          {
            "date": "2021-10-24",
            "count": 2
          }
        ],
        "ym": [
          {
            "date": "2021-10",
            "count": 2
          }
        ],
        "y": [
          {
            "date": "2021",
            "count": 2
          }
        ]
      }
    }
  },
  {
    "address": "DDjPsKCkfgPwrSkAgqcZ4YZR9NTPTz6xFB",
    "counters": {
      "block_counters": [
        4,
        4,
        4,
        4,
        4
      ]
    }
  },
  {
    "address": "D9KY88xQV7SGevxcViREuWCVYdAyh2D4h1",
    "counters": {
      "block_counters": [
        2,
        2,
        2,
        2,
        2
      ]
    }
  },
  {
    "address": "DSnE53PN6Z1p1b8vcvDkEWmByiq23Cco4M",
    "counters": {
      "block_counters": [
        2,
        2,
        2,
        2,
        2
      ],
      "date_counters": {
        "ymdH": [
          {
            "date": "2021-10-24 08",
            "count": 2
          }
        ],
        "ymd": [
          {
            "date": "2021-10-24",
            "count": 2
          }
        ],
        "ym": [
          {
            "date": "2021-10",
            "count": 2
          }
        ],
        "y": [
          {
            "date": "2021",
            "count": 2
          }
        ]
      }
    }
  }
]

```

```json (balances)
  
{
    "a": "D5B883rADzdGGCPtFTEk6xCnTQi1hPkhBt",
    "dc": [
      {
        "d": 1562029200,
        "r": 36400000000,
        "s": 0,
        "ss": 0
      },
      {
        "d": 1563498000,
        "r": 36600000000,
        "s": 36400000000,
        "ss": 36600000000
      },
      {
        "d": 1563937200,
        "r": 36800000000,
        "s": 36600000000,
        "ss": 36800000000
      },
      {
        "d": 1565017200,
        "r": 37000000000,
        "s": 36800000000,
        "ss": 37000000000
      },
      {
        "d": 1565697600,
        "r": 37200000000,
        "s": 37000000000,
        "ss": 37200000000
      },
}
```
