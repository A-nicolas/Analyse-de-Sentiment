pipeline {
    agent any
    
    stages {
        stage('Checkout') {
            steps {
                // Récupérer le code depuis le référentiel git
                git 'https://github.com/A-nicolas/Analyse-de-Sentiment.git'
            }
        }
        stage('Deploy') {
            steps {
                // Connexion à Databricks (à configurer au préalable)
                sh 'databricks configure --token'
                
                // Importer le script sur Databricks
                sh 'databricks workspace import -f notebook_app.py /Workspace/Repos/nicolas@lepont-learning.com/Analyse-de-Sentiment/notebook_app.py'
            }
        }
    }
    
}
