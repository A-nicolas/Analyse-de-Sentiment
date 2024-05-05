pipeline {
    agent any
    
    stages {
        stage('Checkout') {
            steps {
                // Récupérer le code depuis le référentiel git
                git 'https://github.com/A-nicolas/Analyse-de-Sentiment.git'
            }
        }
        stage('Build') {
            steps {
                // Installation des dépendances Python
                sh 'pip install -r requirements.txt'
            }
        }
    }
    
}
