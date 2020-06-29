# Local:
#
# 1. Download wikipedia_assistant_server_key_pair.pem PEM key (sent in my original email) into a private directory
#
# 2. cd to that dir and run chmod 400 wikipedia_assistant_server_key_pair.pem
#
# 3. ssh into machine using ssh -i "wikipedia_assistant_server_key_pair.pem" ec2-user@ec2-18-223-99-67.us-east-2.compute.amazonaws.com

cd {PEM_KEY_DIR}

chmod 400 wikipedia_assistant_server_key_pair.pem

ssh -i "wikipedia_assistant_server_key_pair.pem" ec2-user@ec2-18-223-99-67.us-east-2.compute.amazonaws.com
