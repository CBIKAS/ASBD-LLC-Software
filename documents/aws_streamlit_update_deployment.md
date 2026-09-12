# Deploying Streamlit Updates

Use this procedure for normal updates to the existing EC2 deployment.

## Deployment Details

```text
Host: ubuntu@34.229.133.255
Repository: /home/ubuntu/src/ASBD-LLC-Software
Branch: main
Container: bold_northcutt
Image: dst-csv:latest
App port: 127.0.0.1:8501
Public URL: https://dstcsv.com
AWS region: us-east-1
```

Use the current Elastic IP or DNS name instead of the example host if it changes.

## 1. Test And Push Locally

Check the worktree first:

```bash
git status --short
git pull --ff-only origin main
```

Run relevant tests and launch the app if practical:

```bash
python -m compileall main.py idc_dst_12b1_rep_allocation
streamlit run main.py
```

Commit and push only intended files:

```bash
git add main.py idc_dst_12b1_rep_allocation/report.py requirements.txt Dockerfile
git commit -m "Describe the application update"
git push origin main
```

Adjust the `git add` list to match the actual change. Review `git status` before committing so unrelated files are not included.

## 2. Confirm SSH Access

The security-group port-22 rule must allow the current public IP as a `/32`:

```bash
export AWS_PAGER=""
curl -4 https://checkip.amazonaws.com
aws ec2 describe-security-groups \
  --region us-east-1 \
  --group-ids YOUR_SECURITY_GROUP_ID \
  --query 'SecurityGroups[0].IpPermissions[?FromPort==`22`].IpRanges' \
  --output json
```

If the address changed, add the new rule before revoking the old one. Do not open SSH to `0.0.0.0/0`.

Connect:

```bash
ssh -i ~/.ssh/dstcsv-key.pem ubuntu@YOUR_EC2_PUBLIC_IP
```

## 3. Pull The Exact Commit

On the instance:

```bash
cd /home/ubuntu/src/ASBD-LLC-Software
git status --short --branch
git fetch origin main
git pull --ff-only origin main
git rev-parse --short HEAD
```

Stop if the server has local changes or the fast-forward pull fails. Resolve that deliberately before deploying.

## 4. Build And Replace The Container

Preserve the running image, then build the new one:

```bash
docker tag dst-csv:latest dst-csv:previous
docker build -t dst-csv:latest .
```

Replace the container only after the build succeeds:

```bash
docker rm -f bold_northcutt
docker run -d \
  --name bold_northcutt \
  --restart unless-stopped \
  -p 127.0.0.1:8501:8501 \
  dst-csv:latest
```

This creates a short interruption while the old container is replaced. The localhost-only binding keeps port 8501 off the public network.

## 5. Validate Immediately

```bash
docker ps --filter name=bold_northcutt
docker logs --tail 50 bold_northcutt
curl -I http://127.0.0.1:8501/
sudo nginx -t
curl -I https://dstcsv.com/
```

Expected results:

- The container is `Up` and uses `dst-csv:latest`.
- Streamlit logs show its URL without a traceback.
- Local and public checks return `200`.
- `nginx -t` reports successful syntax validation.

Then test the browser workflow: load the page, upload a representative CSV, submit it, and confirm the report renders. Check that `/_stcore/stream` does not fail in the browser console.

## 6. Roll Back If Needed

Inspect logs first:

```bash
docker logs --tail 200 bold_northcutt
```

Restore the image preserved before the update:

```bash
docker rm -f bold_northcutt
docker run -d \
  --name bold_northcutt \
  --restart unless-stopped \
  -p 127.0.0.1:8501:8501 \
  dst-csv:previous
```

Validate again. Keep the previous image until the new deployment passes a real browser test.

## 7. After A Successful Update

Record the deployed commit and image state:

```bash
git rev-parse HEAD
docker image inspect dst-csv:latest --format '{{.Id}} {{.Created}}'
docker ps --filter name=bold_northcutt
```

Remove the previous image later, when rollback is no longer needed:

```bash
docker image rm dst-csv:previous
```

## Common Issues

### SSH times out

Check the current public IP and security-group port-22 rule. The instance public IP can change after stop/start unless an Elastic IP is attached.

### `git pull` asks for credentials

The server remote must use a public repository or a configured deploy key. Do not paste tokens into commands or commit them.

### Page loads but interactions fail

Check that the active HTTPS Nginx block includes:

```nginx
proxy_http_version 1.1;
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection "upgrade";
```

Run `sudo nginx -t` and reload only after an intentional config change:

```bash
sudo systemctl reload nginx
```

### Build fails

Read the first dependency or source error in the Docker build output. Fix and test locally, push a new commit, then repeat from the pull step. Do not replace the running container with an image that did not build successfully.
