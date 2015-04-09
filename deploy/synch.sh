# Rebuild examples
echo 'Build examples...'
echo ''
cd ../examples && ./gradlew seep && cd -

# Copy all the jars
echo ''
echo 'Copy all the jars'
find ../seep-master -name "*.jar" -exec cp '{}' . \;
find ../seep-worker -name "*.jar" -exec cp '{}' . \;
find ../examples -name "*.jar" ! -name "*gradle*" -exec cp '{}' . \;

