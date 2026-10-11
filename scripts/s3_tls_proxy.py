#!/usr/bin/env python3
"""Owned private-network TLS front end; do not log credentials or signatures."""
import argparse
import json
import ssl
from s3_latency_proxy import Handler, Model, Proxy


class AuditHandler(Handler):
    def forward(self):
        self.server.record({'request_method':self.command,
            'create_condition':self.headers.get('If-None-Match'),
            'signed':self.headers.get('Authorization','').startswith('AWS4-HMAC-SHA256 '),
            'session_token_present':self.headers.get('x-amz-security-token') is not None})
        super().forward()

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = forward


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host',required=True)
    parser.add_argument('--certificate',required=True)
    parser.add_argument('--key',required=True)
    args = parser.parse_args()
    server = Proxy((args.host,0),'http://127.0.0.1:8333',Model(scale=0))
    server.RequestHandlerClass = AuditHandler
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(args.certificate,args.key)
    server.socket = context.wrap_socket(server.socket,server_side=True)
    print(json.dumps({'ready':True,'host':args.host,'port':server.server_port}),flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
