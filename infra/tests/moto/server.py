"""Moto server for the Terraform run in CI, with one fix.

Moto creates EMR Serverless applications in the STARTED state, while AWS
creates them in CREATED, the state the AWS provider waits for. Everything
else is stock Moto:

    docker run -p 5000:5000 -v $PWD/infra/tests/moto/server.py:/server.py \
        --entrypoint python motoserver/moto:<version> /server.py -H 0.0.0.0 -p 5000
"""

from moto.emrserverless import models as emr_serverless
from moto.server import main

emr_serverless.APPLICATION_STATUS = "CREATED"

if __name__ == "__main__":
    main()
