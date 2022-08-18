# Examples

These examples are intended to be run either on our provided docker environment or on your own cluster. 

If you want to try these examples on our docker environment, then:
1. Clone the project if you haven't already
```
git clone https://github.com/vertica/spark-connector.git
```
2. Install sbt on your local machine with JDK 11
3. Start the appropriate configuration from the `spark-connector/docker/` folder
```
docker-compose up -d
# or
docker-compose -f docker-compose-kerberos.yml up -d
```
4. Get a shell to the client container
```
docker exec -it spark-connector-client-1 bash
# or
docker exec -it client bash
```

Once in the container, navigate to the examples folder using `cd /spark-connector/examples`.

You can find more information about our docker environment [here](/docker/README.md).

### Troubleshooting

If you are using the thin jar and running into an error similar to the following:
`java.lang.NoSuchMethodError: 'void cats.kernel.CommutativeSemigroup.$init$(cats.kernel.CommutativeSemigroup)'`, you may need to shade the cats dependency in your project.

This can be done by adding the following to your build.sbt file:

```
assembly / assemblyShadeRules := {
    val shadePackage = "com.azavea.shaded.demo"
    Seq(
        ShadeRule.rename("cats.kernel.**" -> s"$shadePackage.cats.kernel.@1").inAll
    )
} 
```

### Tear down containers

To shut down and remove the containers safely, navigate to `spark-connector/docker/` on your local machine. Then run:
```
docker-compose down
```

If you are running a Kerberos environment, then use 
```
docker compose -f docker-compose-kerberos.yml down
```
