import React from 'react';
import styles from './styles.module.css';
import Feature from './feature';

const FeatureList = [
  {
    title: '1. Data preparation',
    url: 'img/projects.png',
    link:'/APARI_Federated_Learning/docs/category/data-preparation',
    description: (
      <>
        Prepare your Institution's OMOP database for the APARI Model.
      </>
    ),
  },

  {
    title: '2. Model Validation',
    url: 'img/data_processing.png',
    link:'/APARI_Federated_Learning/docs/category/model-validation',
    description: (
      <>
        Use Docker to Train and Test the APARI modal locally with OMOP database.
      </>
    ),
  },

  {
    title: '3. Local FL Simulation',
    url: 'img/datasets.png',
    link:'/APARI_Federated_Learning/docs/Federated%20Learning/localSimulation',
    description: (
      <>
        Simulate the federated learning experiments with local data before moving to production.
      </>
    ),
  },
  {
    title: '4. Federated Learning Provison',
    url: 'img/federated_learning_pro.png',
    link:'/APARI_Federated_Learning/docs/Federated%20Learning/provision',
    description: (
      <>
        Provision your federated learning environment.
      </>
    ),
  },
  {
    title: '5. Model Training',
    url: 'img/workspaces.png',
    link:'/APARI_Federated_Learning/docs/Federated%20Learning/model_training',
    description: (
      <>
        Train your model on the federated learning platform.
      </>
    ),
  },

  {
    title: '6. Model Testing',
    url: 'img/cross_validation.png',
    link:'/APARI_Federated_Learning/docs/Federated%20Learning/cross_validation',
    description: (
      <>
        Test your model with federated cross validation.
      </>
    ),
  },

];

export default function HomepageIcons() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
