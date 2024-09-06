select 
    experiment_id
    ,min(p_first_exposure_date) first_exposure_date
from delta.mlp__experiment_first_exposure__odp.first_exposure 
where 1=1
    and experiment_toggle_id = 'SONIC_LOYALTY_CARDS_NEW' 
group by 1
order by 2 desc

