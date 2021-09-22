@echo off


FOR /F "tokens=* USEBACKQ" %%F IN (`docker inspect -f "{{with index .NetworkSettings.Networks \"EXAMPLE.COM\"}}{{.IPAddress}}{{end}}" hdfs`) DO (
	SET ip_address_var=%%F
)
ECHO %ip_address_var%

