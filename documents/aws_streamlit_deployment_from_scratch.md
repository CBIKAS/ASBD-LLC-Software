# Streamlit AWS Deployment From Scratch

This guide documents the ASBD 12B1 Rep Allocation Streamlit app on a single Ubuntu EC2 instance using Docker, Nginx, and Let's Encrypt.

## Architecture

```text
Porkbun DNS -> EC2 Elastic IP -> Nginx :80/:443 -> Docker on 127.0.0.1:8501 -> Streamlit
```

The app is stateless. Uploaded CSV data is processed in memory and is not intended to be persisted.

## Existing Deployment Values

These describe the current server. Use your own values for a new instance.

| Item | Value |
| --- | --- |
| AWS region | `us-east-1` |
| Instance name | `dstcsv` |
| Instance ID | `i-0bdbb70c9750ae149` |
| Instance type | `t2.micro` |
| Key pair | `dstcsv-key` |
| Repository | `https://github.com/CBIKAS/ASBD-LLC-Software.git` |
| Entrypoint | `main.py` |
| Container image | `dst-csv:latest` |
| Container name | `bold_northcutt` |
| Domain | `dstcsv.com` and `www.dstcsv.com` |

Do not copy these identifiers blindly when creating another instance.

## Prerequisites

- AWS account with an IAM administrator or deployment user. Do not use root credentials for routine work.
- AWS CLI installed and authenticated.
- GitHub repository containing `main.py`, `requirements.txt`, and `Dockerfile`.
- EC2 key pair with a private key stored securely and never committed to Git.
- Porkbun-managed domain.

Set the region and disable the AWS CLI pager:

```bash
aws configure set region us-east-1
export AWS_PAGER=""
aws sts get-caller-identity
```

## 1. Prepare The Repository

The repository should contain a Dockerfile similar to:

```dockerfile
FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

Test locally:

```bash
docker build -t dst-csv:latest .
docker run --rm -p 8501:8501 dst-csv:latest
```

Open `http://localhost:8501` and test a representative CSV upload.

## 2. Launch The EC2 Instance

Launch an Ubuntu LTS EC2 instance in `us-east-1`.

- Use a free-tier-eligible type available to your account, such as `t2.micro`.
- Create or select a key pair and download the private key once.
- Use the default small root volume unless more space is needed.
- Configure the security group with:

| Protocol | Port | Source | Purpose |
| --- | ---: | --- | --- |
| TCP | 22 | Your current public IP as `/32` | SSH administration |
| TCP | 80 | `0.0.0.0/0` | HTTP and redirect to HTTPS |
| TCP | 443 | `0.0.0.0/0` | Public HTTPS application |

Do not expose port 8501 publicly in the final configuration. Nginx proxies to the container locally.

Find your current public IP before adding the SSH rule:

```bash
curl -4 https://checkip.amazonaws.com
```

Use the result as a `/32`, for example `203.0.113.10/32`. If the public IP changes, update the rule before the next SSH session.

For stable DNS, allocate an Elastic IP and associate it with the running instance. An unattached Elastic IP can incur charges, so release it when no longer needed.

## 3. Connect And Install Packages

```bash
chmod 400 ~/.ssh/dstcsv-key.pem
ssh -i ~/.ssh/dstcsv-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
```

On the EC2 instance:

```bash
sudo apt update
sudo apt upgrade -y
sudo apt install -y docker.io git nginx certbot python3-certbot-nginx
sudo systemctl enable --now docker nginx
sudo usermod -aG docker ubuntu
```

Log out and reconnect after adding the user to Docker:

```bash
exit
ssh -i ~/.ssh/dstcsv-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
docker run --rm hello-world
```

## 4. Clone And Build

```bash
sudo mkdir -p /home/ubuntu/src
sudo chown ubuntu:ubuntu /home/ubuntu/src
cd /home/ubuntu/src
git clone https://github.com/CBIKAS/ASBD-LLC-Software.git
cd ASBD-LLC-Software
git branch --show-current
docker build -t dst-csv:latest .
```

For a private repository, configure a deploy key or another non-interactive GitHub authentication method. Never put a GitHub token in shell history.

## 5. Run Streamlit Behind Nginx

Bind Streamlit to localhost only and restart it after a reboot:

```bash
docker run -d \
  --name bold_northcutt \
  --restart unless-stopped \
  -p 127.0.0.1:8501:8501 \
  dst-csv:latest
```

Verify locally:

```bash
curl -I http://127.0.0.1:8501/
docker ps
docker logs --tail 50 bold_northcutt
```

Create `/etc/nginx/sites-available/streamlit`:

```nginx
server {
    listen 80;
    server_name dstcsv.com www.dstcsv.com;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Enable and test it:

```bash
sudo ln -s /etc/nginx/sites-available/streamlit /etc/nginx/sites-enabled/streamlit
sudo nginx -t
sudo systemctl reload nginx
```

The `Upgrade` and `Connection` headers are required for Streamlit's WebSocket endpoint at `/_stcore/stream`.

## 6. Configure Porkbun DNS

Create records pointing to the Elastic IP, or the current public IP if no Elastic IP exists:

| Type | Host | Answer |
| --- | --- | --- |
| A | `@` | `YOUR_ELASTIC_IP` |
| A | `www` | `YOUR_ELASTIC_IP` |

Use DNS records, not Porkbun URL forwarding. Forwarding can change the browser URL and complicate HTTPS and WebSockets.

Check resolution:

```bash
nslookup dstcsv.com
nslookup www.dstcsv.com
```

## 7. Enable HTTPS

After DNS resolves to the instance:

```bash
sudo certbot --nginx -d dstcsv.com -d www.dstcsv.com
```

Choose HTTP-to-HTTPS redirection. Verify renewal:

```bash
sudo certbot renew --dry-run
curl -I https://dstcsv.com/
```

Certbot adds the TLS server block. Ensure the HTTPS block retains the proxy settings, including the WebSocket upgrade headers.

## 8. Final Validation

```bash
curl -I http://127.0.0.1:8501/
curl -I https://dstcsv.com/
sudo nginx -t
docker ps --filter name=bold_northcutt
```

In a browser, test a CSV upload and confirm that the browser console does not report a failed WebSocket connection to `/_stcore/stream`.

## Operational Notes

- Keep port 8501 out of the security group after Nginx works.
- Keep SSH limited to a current `/32` address.
- Monitor disk and memory on a micro instance.
- Docker logs can consume disk over time; review log retention if needed.
- During updates, preserve the previous image as `dst-csv:previous` for rollback.
