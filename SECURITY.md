# Security Guidelines - Lakehouse Unimed

## 🔐 Configuração de Segurança

Este documento descreve as práticas de segurança e configurações necessárias para o projeto Lakehouse Unimed.

## ⚠️ Variáveis de Ambiente Sensíveis

### Airflow Fernet Key

A `AIRFLOW_FERNET_KEY` é usada pelo Airflow para criptografar senhas e conexões no banco de dados.

**NUNCA** commite a Fernet Key no repositório. Sempre use variáveis de ambiente.

#### Como gerar uma nova Fernet Key:

```python
# Método 1: Usando Python
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Método 2: Usando openssl
openssl rand -base64 32
```

#### Configuração:

1. Copie o arquivo de exemplo:
   ```bash
   cp .env.example .env
   ```

2. Gere uma nova Fernet Key:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

3. Substitua `CHANGE_ME_GENERATE_NEW_KEY` no arquivo `.env` pela chave gerada.

### Outras Variáveis Críticas

- `PG_PASSWORD`: Senha do PostgreSQL
- `MINIO_ROOT_PASSWORD`: Senha do MinIO
- `GRAFANA_ADMIN_PASSWORD`: Senha do admin do Grafana

## 🛡️ Boas Práticas de Segurança

### 1. Gestão de Segredos

- ✅ Use sempre variáveis de ambiente para credenciais
- ✅ Mantenha o arquivo `.env` local (nunca commite)
- ✅ Use senhas complexas (mínimo 12 caracteres)
- ✅ Rotate credenciais regularmente

### 2. Configuração de Rede

- Configure firewalls apropriados
- Use HTTPS em produção
- Limite acesso aos serviços internos
- Configure VPN para acesso remoto

### 3. Monitoramento de Segurança

- Configure alertas para tentativas de acesso inválidas
- Monitore logs de autenticação
- Use ferramentas como GitGuardian para detectar vazamentos

### 4. Backup e Recuperação

- Faça backup das credenciais em local seguro
- Documente procedimentos de recuperação
- Teste regularmente os backups

## 🚨 Incidentes de Segurança

### Se uma credencial for exposta:

1. **Revogue imediatamente** a credencial comprometida
2. **Gere uma nova** credencial
3. **Atualize** todos os sistemas que usam a credencial
4. **Monitore** por uso malicioso
5. **Documente** o incidente para análise

### Contatos de Emergência

- Administrador de Sistema: [seu-email@exemplo.com]
- Equipe de Segurança: [seguranca@exemplo.com]

## 📋 Checklist de Segurança

### Antes de Subir em Produção:

- [ ] Todas as senhas padrão foram alteradas
- [ ] Fernet Key foi gerada e configurada
- [ ] Arquivo `.env` está sendo usado (não `.env.example`)
- [ ] Logs de segurança estão configurados
- [ ] Backup das credenciais foi realizado
- [ ] Firewall está configurado
- [ ] HTTPS está habilitado
- [ ] Monitoramento está ativo

### Manutenção Regular:

- [ ] Revisar logs de acesso mensalmente
- [ ] Atualizar credenciais trimestralmente
- [ ] Verificar atualizações de segurança
- [ ] Testar procedimentos de backup

## 🔍 Ferramentas de Segurança Recomendadas

- **GitGuardian**: Detecção de segredos em repositórios
- **Trivy**: Scanner de vulnerabilidades em containers
- **OWASP ZAP**: Teste de penetração em aplicações web
- **Fail2ban**: Proteção contra ataques de força bruta

## 📚 Recursos Adicionais

- [Airflow Security Guide](https://airflow.apache.org/docs/apache-airflow/stable/security/index.html)
- [Docker Security Best Practices](https://docs.docker.com/develop/security-best-practices/)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)

---

**Importante**: Este documento deve ser atualizado sempre que novas práticas de segurança forem implementadas.