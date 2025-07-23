def build_dim_plot(df):
    from etl.utils.matcher import match
    keys = {
        'entity_id': match(df, ['Entity ID']),
        'plot_number': match(df, ['Existing Plot - Plot number', 'New Plot - Plot number']),
        'species': match(df, ['Species']),
        'variety': match(df, ['Variety']),
        'year_started': match(df, ['Year Started']),
        'polygon_area': match(df, ['Calculated Polygon Area (Ha)', 'Calculate Polygon Area']),
        'total_coffee_area': match(df, ['Total Coffee Area']),
        'coffee_area_active': match(df, ['Coffee Area Under Active Production']),
        'coffee_area_rejuvenation': match(df, ['Coffee Area Under Rejuvenation Stage']),
        'coffee_area_intercropping': match(df, ['Coffee Area Intercropping']),
        'intercropping_crops': match(df, ['Intercropping crops']),
    }
    out = df[list(keys.values())].copy().rename(columns={v: k for k, v in keys.items()})
    out.insert(0, 'plot_id', range(1, len(out)+1))
    return out.drop_duplicates().reset_index(drop=True)
