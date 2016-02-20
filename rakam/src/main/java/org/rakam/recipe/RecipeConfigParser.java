package org.rakam.recipe;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class RecipeConfigParser {

    public RecipeConfigParser() {
        Constructor constructor = new Constructor(Recipe.class);
        TypeDescription carDescription = new TypeDescription(Recipe.class);
        constructor.addTypeDescription(carDescription);
        Yaml yaml = new Yaml(constructor);
        Recipe recipe = (Recipe) yaml.load("ecommerce_test.yml");
    }

}
