SET TimeZone = 'America/New_York';  -- the trips happened in NYC
CREATE SECRET (TYPE s3, PROVIDER credential_chain, CHAIN 'env',
               ENDPOINT 'localhost:8333', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
ATTACH 'xenny' AS lake (TYPE iceberg, URI 'http://localhost:8181/catalog',
                        AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none');
USE lake.bronze;
