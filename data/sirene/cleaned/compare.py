import pandas as pd
import numpy as np
import pandera as pa

import matplotlib.pyplot as plt


def configure(context):
    context.config("sirene_compare_method_output")
    context.stage("data.sirene.cleaned.eqasim")
    context.stage("data.sirene.cleaned.horizon")
    context.stage("data.sirene.cleaned.adrien")

def execute(context):
    df_cleaned_eqasim = context.stage("data.sirene.cleaned.eqasim")
    df_cleaned_horizon = context.stage("data.sirene.cleaned.horizon")
    df_cleaned_adrien = context.stage("data.sirene.cleaned.adrien")

    schema = pa.DataFrameSchema({
        "siren": pa.Column("int32"),
        "siret": pa.Column(int),
        "municipality_id": pa.Column(str),
        "employees": pa.Column(int),
        "ape": pa.Column(str),
        "law_status": pa.Column(str),
        "st8": pa.Column(int),
        "st20": pa.Column(int),
        "st45": pa.Column(str),
        "geometry": pa.Column("geometry"),
    })
    schema.validate(df_cleaned_eqasim)
    schema.validate(df_cleaned_horizon)
    schema.validate(df_cleaned_adrien)

    print("Number of companies with 'esaqim' method : %s" % len(df_cleaned_eqasim))
    print("Number of companies with 'horizon' method : %s" % len(df_cleaned_horizon))
    print("Number of companies with 'adrien' method : %s" % len(df_cleaned_adrien))

    # Compute the distribution of the employees column
    employees_bins = [0, 1, 3, 6, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, np.inf]

    employees_eqasim = df_cleaned_eqasim['employees'].value_counts(normalize=True, bins=employees_bins, sort=False)
    employees_eqasim.name = 'eqasim (nb: %d)' % len(df_cleaned_eqasim)

    employees_horizon = df_cleaned_horizon['employees'].value_counts(normalize=True, bins=employees_bins, sort=False)
    employees_horizon.name = 'horizon (nb: %d)' % len(df_cleaned_horizon)

    employees_adrien = df_cleaned_adrien['employees'].value_counts(normalize=True, bins=employees_bins, sort=False)
    employees_adrien.name = 'adrien (nb: %d)' % len(df_cleaned_adrien)

    df_employees = pd.concat([employees_eqasim,employees_horizon,employees_adrien], axis=1)
    print(df_employees)

    # Plotting df_employees data
    df_employees.plot(kind='bar', stacked=False)
    plt.xlabel('employees')
    plt.ylabel('Count / Max')
    plt.title('Distribution of employees')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('%s/sirene_compare_employees.png' % context.config("sirene_compare_method_output"))

    # Compute the distribution of the st8 column
    st8_eqasim = df_cleaned_eqasim['st8'].value_counts(normalize=True, sort=False)
    st8_eqasim.name = 'eqasim (nb: %d)' % len(df_cleaned_eqasim)

    st8_horizon = df_cleaned_horizon['st8'].value_counts(normalize=True, sort=False)
    st8_horizon.name = 'horizon (nb: %d)' % len(df_cleaned_horizon)

    st8_adrien = df_cleaned_adrien['st8'].value_counts(normalize=True, sort=False)
    st8_adrien.name = 'adrien (nb: %d)' % len(df_cleaned_adrien)

    df_st8 = pd.concat([st8_eqasim,st8_horizon,st8_adrien], axis=1)
    df_st8.sort_index(inplace=True)
    print(df_st8)

    # Plotting df_st8 data
    df_st8.plot(kind='bar', stacked=False)
    plt.xlabel('st8')
    plt.ylabel('Count / Max')
    plt.title('Distribution of st8 column')
    plt.legend()
    plt.tight_layout()
    plt.savefig('%s/sirene_compare_st8.png' % context.config("sirene_compare_method_output"))

    # Compute the distribution of the st45 column
    st45_eqasim = df_cleaned_eqasim['st45'].value_counts(normalize=True, sort=False)
    st45_eqasim.name = 'eqasim (nb: %d)' % len(df_cleaned_eqasim)

    st45_horizon = df_cleaned_horizon['st45'].value_counts(normalize=True, sort=False)
    st45_horizon.name = 'horizon (nb: %d)' % len(df_cleaned_horizon)

    st45_adrien = df_cleaned_adrien['st45'].value_counts(normalize=True, sort=False)
    st45_adrien.name = 'adrien (nb: %d)' % len(df_cleaned_adrien)

    df_st45 = pd.concat([st45_eqasim, st45_horizon, st45_adrien], axis=1)
    df_st45.sort_index(inplace=True)
    print(df_st45)

    # Plotting df_st45 data
    df_st45.plot(kind='bar', stacked=False)
    plt.xlabel('st45')
    plt.ylabel('Count / Max')
    plt.title('Distribution of st45 column')
    plt.legend()
    plt.tight_layout()
    plt.savefig('%s/sirene_compare_st45.png' % context.config("sirene_compare_method_output"))

    # Compute the distribution of the st20 column
    st20_eqasim = df_cleaned_eqasim['st20'].value_counts(normalize=True, sort=False)
    st20_eqasim.name = 'eqasim (nb: %d)' % len(df_cleaned_eqasim)

    st20_horizon = df_cleaned_horizon['st20'].value_counts(normalize=True, sort=False)
    st20_horizon.name = 'horizon (nb: %d)' % len(df_cleaned_horizon)

    st20_adrien = df_cleaned_adrien['st20'].value_counts(normalize=True, sort=False)
    st20_adrien.name = 'adrien (nb: %d)' % len(df_cleaned_adrien)

    df_st20 = pd.concat([st20_eqasim, st20_horizon, st20_adrien], axis=1)
    df_st20.sort_index(inplace=True)
    print(df_st20)

    # Plotting df_st20 data
    df_st20.plot(kind='bar', stacked=False)
    plt.xlabel('st20')
    plt.ylabel('Count / Max')
    plt.title('Distribution of st20 column')
    plt.legend()
    plt.tight_layout()
    plt.savefig('%s/sirene_compare_st20.png' % context.config("sirene_compare_method_output"))