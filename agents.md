CREATE OR REPLACE FUNCTION clean_for_embedding(s STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
TRIM(
  REGEXP_REPLACE(                                      -- 5) collapse extra spaces
    UPPER(
      REGEXP_REPLACE(                                  -- 4) keep only A–Z, 0–9, space, &
        REGEXP_REPLACE(                                -- 3) remove stopwords ANYWHERE (whole words)
          REGEXP_REPLACE(                              -- 2) remove state suffixes at END
            REGEXP_REPLACE(                            -- 1) remove ONLY trailing numbers / store ids
              s,
              '(?i)\\s*(?:[#/\\-]?\\d{2,}|(?:ST|STORE)\\s*#?\\d+|(?:UNIT|SUITE)\\s*#?\\d+)\\s*$',
              ''
            ),
            '(?i)\\b(VA|VIRGINIA|MD|MARYLAND|DC|MCLEAN)\\b\\s*$',
            ''
          ),
          '(?i)\\b(SINCE|CVENT|INC|ORDER|CX|TIST|ORG|OF|WWW|AMK|SQ|PY|GAITHERSB)\\b',
          ' '
        ),
        '[^A-Z0-9\\s&]',
        ''
      )
    ),
    '\\s+',
    ' '
  )
)
$$;
