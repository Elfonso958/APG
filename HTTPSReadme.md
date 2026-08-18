# HTTPS setup for the APG server

This document records how trusted HTTPS is configured for the APG, LMS, and Simulator applications hosted on `3.107.209.189`.

## Public URLs

- Crew briefing: `https://3.107.209.189/APG/dcs/crew-briefing`
- APG application: `https://3.107.209.189/APG/`
- LMS: `https://3.107.209.189/`
- Simulator: `https://3.107.209.189/Simulator/`

All normal HTTP requests are redirected to HTTPS. The only HTTP path that is not redirected is the Let’s Encrypt validation path:

```text
/.well-known/acme-challenge/
```

## Server layout

- Server: Ubuntu 24.04 LTS
- Reverse proxy: Nginx 1.24
- Public address: `3.107.209.189`
- APG upstream: `127.0.0.1:8081`
- APG service: `apg-importer.service`
- Active Nginx site: `/etc/nginx/sites-enabled/lms`
- ACME webroot: `/var/www/letsencrypt`
- Repository copy of the Nginx configuration: `deploy/nginx-lms-https.conf`

The Nginx configuration enables TLS 1.2 and TLS 1.3 and preserves the existing `/APG`, `/Simulator`, and LMS proxy routes.

## Certificate

The certificate is a publicly trusted Let’s Encrypt IP-address certificate. It is not self-signed.

IP-address certificates use Let’s Encrypt’s `shortlived` profile and are valid for approximately six days. Automatic renewal is therefore essential.

Certificate files:

```text
/etc/letsencrypt/live/3.107.209.189/fullchain.pem
/etc/letsencrypt/live/3.107.209.189/privkey.pem
```

Inspect the current certificate:

```bash
sudo /snap/bin/certbot certificates
sudo openssl x509 \
  -in /etc/letsencrypt/live/3.107.209.189/fullchain.pem \
  -noout -subject -issuer -dates
```

## Certbot

Certbot is installed from Snap because Ubuntu’s APT version does not support IP-address certificates. The installed version at setup time was Certbot 5.7.0.

```bash
/snap/bin/certbot --version
```

The certificate was originally requested with:

```bash
sudo /snap/bin/certbot certonly \
  --preferred-profile shortlived \
  --webroot \
  --webroot-path /var/www/letsencrypt \
  --ip-address 3.107.209.189 \
  --non-interactive \
  --agree-tos \
  --register-unsafely-without-email
```

The ACME challenge must remain publicly reachable over port 80 for renewal.

## Automatic renewal

The Certbot Snap installs a systemd renewal timer. Check it with:

```bash
systemctl list-timers --all | grep -i certbot
systemctl status snap.certbot.renew.timer
```

After a successful renewal, this deploy hook validates and reloads Nginx:

```text
/etc/letsencrypt/renewal-hooks/deploy/reload-nginx-after-certbot.sh
```

The repository copy is at `deploy/reload-nginx-after-certbot.sh`.

Test renewal safely with:

```bash
sudo /snap/bin/certbot renew \
  --cert-name 3.107.209.189 \
  --dry-run \
  --run-deploy-hooks
```

Review renewal logs with:

```bash
sudo journalctl -u snap.certbot.renew.service --no-pager
sudo tail -100 /var/log/letsencrypt/letsencrypt.log
```

## Nginx verification and reload

Always test the configuration before reloading:

```bash
sudo nginx -t
sudo systemctl reload nginx
sudo systemctl status nginx --no-pager
sudo ss -ltnp | grep -E ':(80|443) '
```

Verify HTTPS and the HTTP redirect externally:

```bash
curl -I https://3.107.209.189/APG/dcs/crew-briefing
curl -I http://3.107.209.189/APG/dcs/crew-briefing
```

The HTTPS request should return `200`. The HTTP request should return `301` with an HTTPS `Location` header.

## Backups and recovery

The pre-HTTPS Nginx configuration was backed up to:

```text
/etc/nginx/backups/lms.pre-https-20260818
```

To restore it in an emergency:

```bash
sudo cp /etc/nginx/backups/lms.pre-https-20260818 /etc/nginx/sites-enabled/lms
sudo nginx -t
sudo systemctl reload nginx
```

This restores HTTP-only access. Do not delete the active certificate or Certbot account during routine recovery.

## Updating the Nginx configuration from the repository

From a workstation with SSH access:

```powershell
scp -i "C:\Users\Jayden\.ssh\lightsail.pem" `
  deploy\nginx-lms-https.conf `
  ubuntu@3.107.209.189:/tmp/nginx-lms-https.conf

ssh -i "C:\Users\Jayden\.ssh\lightsail.pem" ubuntu@3.107.209.189 `
  "sudo install -o root -g root -m 644 /tmp/nginx-lms-https.conf /etc/nginx/sites-enabled/lms && sudo nginx -t && sudo systemctl reload nginx"
```

## Important cautions

- Do not close port 80. Let’s Encrypt uses it for the webroot challenge.
- Do not disable the Certbot renewal timer. The certificate lasts only about six days.
- Do not expose the private key or commit `/etc/letsencrypt` contents to Git.
- If the public IP changes, request a new certificate for the new IP and update the Nginx certificate paths.
- A stable DNS name is still preferable long-term because it allows the server address to change without changing the application URL or certificate identity.
- Check certificate-expiry monitoring regularly; automated renewal should be treated as production-critical.

## Initial setup record

- HTTPS enabled: 18 August 2026
- Initial production certificate expiry: 25 August 2026
- Certificate authority: Let’s Encrypt
- Certificate identifier: IPv4 address `3.107.209.189`
- Certbot version: 5.7.0
