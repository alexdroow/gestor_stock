# Subir Proyecto a GitHub

## 1) Instalar Git (si no está instalado)
- Descarga: https://git-scm.com/download/win
- Reinicia terminal después de instalar.

## 2) Inicializar repositorio (en esta carpeta)
```powershell
cd C:\Users\seanv\OneDrive\Documentos\Python\gestor_stock
git init
git branch -M main
```

## 3) Configurar identidad
```powershell
git config user.name "Tu Nombre"
git config user.email "tu_correo@dominio.com"
```

## 4) Crear primer commit
```powershell
git add .
git commit -m "v4.0 - limpieza de codificación y preparación GitHub"
```

## 5) Conectar con repositorio remoto
```powershell
git remote add origin https://github.com/TU_USUARIO/TU_REPO.git
git push -u origin main
```

## Notas importantes
- `.gitignore` ya excluye base de datos, backups, builds, temporales y carpetas locales.
- `stock.db` y `facturas/` no se subirán (dato local).
- Para publicar ejecutables, usa **GitHub Releases** en vez de versionar `dist/`.
