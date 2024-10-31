async function makePrediction() {
    const formData = new FormData(document.getElementById("predictForm"));
    const data = {};
    formData.forEach((value, key) => {
        if (value) {
            data[key] = parseFloat(value);
        }
    });

    try {
        const response = await fetch('/predict', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify([data])
        });
        
        const result = await response.json();
        document.getElementById("predictionResult").textContent = "Prediction: " + result.predictions;
    } catch (error) {
        document.getElementById("predictionResult").textContent = "Erreur: " + error.message;
    }
}
