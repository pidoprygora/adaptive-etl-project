# Kubernetes доступ через SSH tunnel

Цей стенд налаштований так, що Kubernetes API (`6443`) слухає **лише на localhost EC2** (`127.0.0.1:6443`), тому доступ іде тільки через SSH tunnel.

## 1) Підніми tunnel з локальної машини

```bash
ssh -i ~/.ssh/<your_key> -N -L 6443:127.0.0.1:6443 ubuntu@<EC2_PUBLIC_IP>
```

- `-N` — не запускати shell-команди, лише тунель.
- `-L 6443:127.0.0.1:6443` — локальний порт `6443` прокидається до `127.0.0.1:6443` на EC2.

Тримай це вікно відкритим під час роботи з `kubectl`.

## 2) Скопіюй kubeconfig з EC2

```bash
scp -i ~/.ssh/<your_key> ubuntu@<EC2_PUBLIC_IP>:/home/ubuntu/.kube/config ./kubeconfig-k3d
```

Після копіювання у файлі має бути server URL виду:

```yaml
server: https://0.0.0.0:6443
```

або `https://127.0.0.1:6443`. Якщо там інший host, заміни його на:

```yaml
server: https://127.0.0.1:6443
```

## 3) Використовуй kubeconfig локально

```bash
export KUBECONFIG=$PWD/kubeconfig-k3d
kubectl get nodes
kubectl get pods -A
```

## 4) Підключення до Airflow Web UI через SSH tunnel

На EC2 виконай port-forward:

```bash
kubectl -n airflow port-forward svc/airflow-webserver 8080:8080
```

Потім з локальної машини відкрий ще один SSH tunnel:

```bash
ssh -i ~/.ssh/<your_key> -N -L 8080:127.0.0.1:8080 ubuntu@<EC2_PUBLIC_IP>
```

UI буде доступний на [http://localhost:8080](http://localhost:8080).

## 5) Перевірка типових проблем

- `Unable to connect to the server`: перевір, що SSH tunnel на `6443` живий.
- `certificate signed by unknown authority`: використовуй оригінальний kubeconfig, не видаляй `certificate-authority-data`.
- `connection refused`: перевір на EC2 `k3d cluster list` і `kubectl get nodes`.
- `timeout`: перевір Security Group (доступ до SSH з твого CIDR).
