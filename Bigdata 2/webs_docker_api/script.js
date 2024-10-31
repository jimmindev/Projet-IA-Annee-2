// Attendez que le DOM soit complètement chargé
document.addEventListener('DOMContentLoaded', function() {
    // Écouteur d'événements pour la soumission du formulaire
    document.getElementById('prediction-form').addEventListener('submit', function(event) {
        event.preventDefault(); // Empêche l'envoi normal du formulaire

        const formData = new FormData(this); // Récupère les données du formulaire
        const data = {};

        // Convertir les données du formulaire en objet avec les valeurs
        formData.forEach((value, key) => {
            data[key] = parseInt(value); // Crée un objet avec les valeurs du formulaire converties en int
        });

        // Afficher les données dans la console pour débogage
        console.log("Données à envoyer:", data);
        
        // Envoyer les données à l'API Flask
        fetch('http://127.0.0.1:5000/predict', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json', // Spécifier que les données envoyées sont en JSON
            },
            body: JSON.stringify(data), // Convertir l'objet data en chaîne JSON
        })
        .then(response => {
            if (!response.ok) {
                throw new Error('Erreur réseau lors de la requête à l\'API');
            }
            return response.json(); // Convertir la réponse en JSON
        })
        .then(result => {
            // Affichez le résultat dans la zone de résultat
            document.getElementById('result').innerText = `Prédiction: ${result.predictions[0]}`;
        })
        .catch(error => {
            console.error('Erreur:', error);
            document.getElementById('result').innerText = 'Une erreur est survenue lors de la prédiction.';
        });
    });

    // Fonction pour sélectionner des options aléatoires
    document.getElementById('randomize-button').addEventListener('click', function() {
        const radioGroups = [
            'bruises_encoded',
            'cap-color_encoded',
            'cap-shape_encoded',
            'cap-surface_encoded',
            'gill-attachment_encoded',
            'gill-color_encoded',
            'gill-count_encoded',
            'gill-size_encoded',
            'gill-spacing_encoded',
            'habitat_encoded',
            'odor_encoded',
            'population_encoded',
            'ring-number_encoded',
            'ring-type_encoded',
            'spore-print-color_encoded',
            'stalk-color-above-ring_encoded',
            'stalk-color-below-ring_encoded',
            'stalk-root_encoded',
            'stalk-shape_encoded',
            'stalk-surface-above-ring_encoded',
            'stalk-surface-below-ring_encoded',
            'veil-color_encoded',
            'veil-texture_encoded',
            'veil-type_encoded'
        ];

        const results = randomizeSelections(radioGroups);

         // Afficher les résultats dans la console
        console.log('Sélections aléatoires :', results.join(', '));
    });

    // Fonction pour randomiser les sélections
    function randomizeSelections(groups) {
        const results = [];
        
        groups.forEach(group => {
            const radios = document.getElementsByName(group);
            if (radios.length > 0) { // Vérifiez si le groupe de boutons radio existe
                const randomIndex = Math.floor(Math.random() * radios.length);
                radios[randomIndex].checked = true; // Sélectionner une option aléatoire
                
                // Ajouter la sélection à la liste des résultats
                results.push(radios[randomIndex].nextSibling.nodeValue.trim()); // Récupérer le texte de l'option
            } else {
                console.warn(`Aucun bouton radio trouvé pour le groupe: ${group}`);
            }
        });

        return results;
    }
});
