import pandas as pd
def get_from_oecd(sdmx_query):
    return pd.read_csv(
        f"https://stats.oecd.org/SDMX-JSON/data/{sdmx_query}?contentType=csv"
    )
data = (get_from_oecd("LAB_REG_VAC/SWE+DEN+FIN+NOR.LMUNRLTT_STSA.M/"))
df = pd.DataFrame(data)
df_new = df.pivot(index='TIME', columns='Country', values='Value')
df_new2 = df_new.tail(12)
df_new2.to_csv('data/OECDledigestillinger.csv', index=True)
