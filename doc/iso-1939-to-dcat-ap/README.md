# XLS Info
### Configuración
Los pasos necesarios para configurar el entorno e instalar las librerías son los siguientes. En primer lugar crear el directorio `venv` en donde se vaya a ejecutar:

```bash
# Windows
pip install virtualenv
cd iso-1939-to-dcat-ap
python -m venv env
env\Scripts\activate.bat
pip install -r requirements.txt
```


### Ejecución
Antes de ejecutar se pueden editar los parametros de `./iso-1939-to-dcat-ap/config.yml` que se quieran.

>**Warning**<br>
>Verifica que estes en el entorno virtual, te debe salir un `(env) ` antes del directorio en el que te encuentres en CMD, algo parecido a: `(env) directorio\que\sea\iso-19139-to-dcat-ap\iso-1939-to-dcat-ap>`
>```bash
>cd iso-1939-to-dcat-ap
>env\Scripts\activate.bat
>```

Vamos al directorio `src` y ejecutamos el script
```bash
cd src
python xlst_mapper/xlst_sample/xlst_management.py
```